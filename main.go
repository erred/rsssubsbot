package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/signal"
	"sort"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"cloud.google.com/go/storage"
	tapi "github.com/go-telegram-bot-api/telegram-bot-api"
	"github.com/mmcdole/gofeed"
	log "github.com/sirupsen/logrus"
)

var (
	APIToken = os.Getenv("TELEGRAM_TOKEN")
	Bucket   = os.Getenv("BUCKET")
)

func init() {
	switch strings.ToLower(os.Getenv("LOG_LEVEL")) {
	case "debug":
		log.SetLevel(log.DebugLevel)
	case "info":
		log.SetLevel(log.InfoLevel)
	case "error":
		fallthrough
	default:
		log.SetLevel(log.ErrorLevel)
	}
	log.Debugln("Log level set to", log.GetLevel())

	switch strings.ToLower(os.Getenv("LOG_FORMAT")) {
	case "json":
		log.SetFormatter(&log.JSONFormatter{})
		log.Debugln("Log format set to json")
	default:
		log.SetFormatter(&log.TextFormatter{})
		log.Debugln("Log format set to text")
	}
}

func main() {
	s, err := NewServer(APIToken)
	if err != nil {
		log.Fatal("main new server", err)
	}
	defer s.Export()

	go s.Respond()
	go func(d time.Duration) {
		s.update()
		s.Export()
		for range time.NewTicker(d).C {
			s.update()
			s.Export()
		}
	}(15 * time.Minute)

	sigs := make(chan os.Signal)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGKILL)
	sig := <-sigs
	log.Infoln("main ending with", sig)
}

type Server struct {
	bot   *tapi.BotAPI
	store *storage.Client

	Feeds
	Seens
}

func NewServer(token string) (*Server, error) {
	log.Debugln("NewServer creating")
	fn := "rsssubsbot.json"

	log.Debugln("NewServer creating telegram bot")
	bot, err := tapi.NewBotAPI(token)
	if err != nil {
		return nil, fmt.Errorf("NewServer new bot %v", err)
	}

	log.Debugln("NewServer creating storage client")
	store, err := storage.NewClient(context.Background())
	if err != nil {
		return nil, fmt.Errorf("NewServer storage client: %v", err)
	}

	s := &Server{
		bot:   bot,
		store: store,
		Feeds: NewFeeds(),
		Seens: NewSeens(),
	}
	log.Debugln("NewServer retrieving from storage")
	r, err := store.Bucket(Bucket).Object(fn).NewReader(context.Background())
	if err == nil {
		defer r.Close()
		log.Debugln("NewServer decoding from storage")
		err = json.NewDecoder(r).Decode(s)
		if err != nil {
			log.Debugln("Newserver decode from storage", err)
		}
	}
	return s, nil
}

func (s *Server) Export() {
	fn := "rsssubsbot.json"
	log.Debugln("Export to", Bucket, fn)
	w := s.store.Bucket(Bucket).Object(fn).NewWriter(context.Background())
	defer w.Close()
	err := json.NewEncoder(w).Encode(s)
	if err != nil {
		log.Errorln("Export to bucket", err)
		return
	}
	log.Infoln("Export done")
}

func (s *Server) Respond() {
	log.Debugln("Respond starting")
	updates, err := s.bot.GetUpdatesChan(tapi.NewUpdate(0))
	if err != nil {
		log.Fatal("Respond get updates", err)
	}
	for update := range updates {
		if update.Message == nil {
			continue
		}

		ss := strings.Fields(update.Message.Text)
		if len(ss) == 0 {
			continue
		}

		log.Debugln("Respond processing message")
		var txt string
		switch strings.TrimPrefix(strings.ToLower(ss[0]), "/") {
		case "sub", "subscribe", "add":
			if len(ss) < 2 {
				txt = "Please provide a url to subscribe to"
			} else {
				txt = "Subscribed to " + strconv.Itoa(len(ss)-1) + " feeds"
				s.Seens.CheckSeen(update.Message.Chat.ID)
				for _, sss := range ss[1:] {
					go s.Feeds.Add(sss, update.Message.Chat.ID)
				}
			}
		case "unsub", "unsubscribe", "rm":
			if len(ss) < 2 {
				txt = "Please provide a url to unsubscribe from"
			} else {
				txt = "Unsubscribed from " + ss[1]
				err := s.Feeds.Rm(ss[1], strings.Join(ss[1:], " "), update.Message.Chat.ID)
				if err != nil {
					txt = "Error unsubscribing from " + ss[1] + ": " + err.Error()
				}
			}
		case "list", "show", "subs":
			subs := s.Feeds.List(update.Message.Chat.ID)
			txt = "You are subscribed to:\n\n" + strings.Join(subs, "\n")
		case "update":
			if update.Message.From.UserName == "seankhliao" {
				go s.update()
				txt = "update started"
			} else {
				txt = update.Message.From.UserName + " is not authorized to update"
			}
		case "start", "help":
			fallthrough
		default:
			txt = `Hello
I'm RSS Subs
here's what I can do:

sub <url>: subscribe to rss feed @ url
unsub <url>: unsubscribe to rss feed @url
subs: show subscriptions
help: show this message`
		}

		_, err = s.bot.Send(tapi.NewMessage(update.Message.Chat.ID, txt))
		if err != nil {
			log.Errorln("Respond send msg", err)
		}
	}
}

func (s *Server) update() {
	log.Debugln("update starting")
	defer log.Infoln("update done")

	var wg sync.WaitGroup
	sends := make(chan tapi.MessageConfig, 16)

	go func() {
		for m := range sends {
			_, err := s.bot.Send(m)
			if err != nil {
				log.Errorln("update 1 sends", err)
			}
		}
	}()

	t := time.Now().Add(-192 * time.Hour)
	ts := NewArticleKey("", &t, nil)
	type h struct {
		a  ArticleKey
		it *gofeed.Item
	}

	for url, feed := range s.Feeds {
		wg.Add(1)
		go func(url string, feed *Feed) {
			defer wg.Done()

			f, err := gofeed.NewParser().ParseURL(url)
			if err != nil {
				log.Errorln("update 2 feed parseurl", url, err)
				return
			}

			for cid := range feed.Chats {
				var q []h
				for _, it := range f.Items {
					a := NewArticleKey(it.Title, it.PublishedParsed, it.UpdatedParsed)
					if !s.Seens.MarkSeen(cid, a) {
						if a > ts {
							q = append(q, h{a, it})
						}
					}
				}
				sort.Slice(q, func(i, j int) bool {
					return q[i].a < q[j].a
				})
				for _, it := range q {
					sends <- tapi.NewMessage(cid, it.it.Link)
				}
			}
		}(url, feed)
	}

	wg.Wait()
	close(sends)
}

// Feeds is a mapping of urls to Feed
type Feeds map[string]*Feed

func NewFeeds() Feeds {
	return make(Feeds)
}

// New creates a new feed
func (f Feeds) New(url string) error {
	feed, err := gofeed.NewParser().ParseURL(url)
	if err != nil {
		return fmt.Errorf("gofeed parse %v", err)
	}
	f[url] = NewFeed(feed.Title)
	return nil
}

func (f Feeds) Add(url string, cid int64) error {
	if _, ok := f[url]; !ok {
		err := f.New(url)
		if err != nil {
			log.Errorln("feeds add", err)
			return fmt.Errorf("feeds add %v", err)
		}
	}
	f[url].Add(cid)
	return nil
}

func (f Feeds) Rm(url, title string, cid int64) error {
	if url != "" {
		if _, ok := f[url]; ok {
			f[url].Rm(cid)
			return nil
		}
	}
	title = strings.ToLower(title)
	for u, feed := range f {
		if strings.Contains(strings.ToLower(feed.Title), title) {
			f[u].Rm(cid)
			return nil
		}
	}
	return fmt.Errorf("No matching subscription found")
}

func (f Feeds) List(cid int64) []string {
	var arr []string
	for u, feed := range f {
		if _, ok := feed.Chats[cid]; ok {
			arr = append(arr, feed.Title+": "+u)
		}
	}
	return arr
}

// Feed is a collection of subscribers and the feed name
type Feed struct {
	// Set of chats subscribed to this feed
	Chats map[int64]struct{}
	// name of feed
	Title string
}

func NewFeed(title string) *Feed {
	return &Feed{
		Chats: make(map[int64]struct{}),
		Title: title,
	}
}

func (f *Feed) Add(cid int64) {
	f.Chats[cid] = struct{}{}
}
func (f *Feed) Rm(cid int64) {
	delete(f.Chats, cid)
}

type ArticleKey string

func NewArticleKey(title string, ts, up *time.Time) ArticleKey {
	if up != nil {
		ts = up
	}
	return ArticleKey(ts.Format(time.RFC3339) + "-" + title)
}

// Seens is a mapping of ChatID to Seen
type Seens map[int64]Seen

func NewSeens() Seens {
	return make(Seens)
}
func (s Seens) CheckSeen(cid int64) {
	if s[cid] == nil {
		s[cid] = make(Seen)
	}
}

// MarkSeen marks an article as seen
// returns if it was originally seen
func (s Seens) MarkSeen(cid int64, a ArticleKey) bool {
	var ok bool
	if s[cid] == nil {
		s[cid] = make(Seen)
	}
	if _, ok = s[cid][a]; !ok {
		s[cid][a] = struct{}{}
	}
	return ok
}

// Seen is a set of articles a chat has already seen
type Seen map[ArticleKey]struct{}
