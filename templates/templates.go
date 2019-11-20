package templates

import (
	"encoding/json"
	"fmt"
	"html/template"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/cloudkarafka/cloudkarafka-manager/config"
	"github.com/cloudkarafka/cloudkarafka-manager/log"
	"github.com/cloudkarafka/cloudkarafka-manager/server/cookie"
	m "github.com/cloudkarafka/cloudkarafka-manager/server/middleware"
	humanize "github.com/dustin/go-humanize"
)

var templates map[string]*template.Template
var defaultTmpl = `{{define "main" }} {{ template "default" . }} {{ end }}`

var templateFuncs = template.FuncMap{
	"toS": func(value interface{}) string {
		switch v := value.(type) {
		case string:
			return v
		case int:
			return strconv.Itoa(v)
		default:
			return ""
		}
	},
	"toLower": func(str string) string {
		return strings.ToLower(str)
	},
	"commaList": func(items []int) string {
		return strings.Trim(strings.Join(strings.Fields(fmt.Sprint(items)), ","), "[]")
	},
	"throughput": func(value int) string {
		if value == -1 {
			return "No data"
		}
		return fmt.Sprintf("%s/s", humanize.Bytes(uint64(value)))
	},
	"size": func(value int) string {
		return humanize.Bytes(uint64(value))
	},
	"number": func(value int) string {
		return humanize.Comma(int64(value))
	},
}

type TemplateError struct {
	s string
}

func (e *TemplateError) Error() string {
	return e.s
}

func NewError(text string) error {
	return &TemplateError{text}
}

func Load() error {

	templates = make(map[string]*template.Template)
	layoutFiles, err := filepath.Glob("templates/layouts/*.html")
	if err != nil {
		return err
	}
	includeFiles, err := filepath.Glob("templates/views/*.html")
	if err != nil {
		return err
	}
	snippetFiles, err := filepath.Glob("templates/snippets/*.html")
	if err != nil {
		return err
	}
	defaultTemplate := template.New("default")
	defaultTemplate, err = defaultTemplate.Parse(defaultTmpl)
	if err != nil {
		return err
	}
	for _, file := range includeFiles {
		fileName := filepath.Base(file)
		extension := filepath.Ext(fileName)
		name := fileName[0 : len(fileName)-len(extension)]
		templates[name], err = defaultTemplate.Clone()
		if err != nil {
			return err
		}
		files := append(append(layoutFiles, file), snippetFiles...)
		templates[name] = template.Must(templates[name].Funcs(templateFuncs).ParseFiles(files...))
	}
	pageFiles, err := filepath.Glob("templates/pages/*.html")
	if err != nil {
		return err
	}
	for _, file := range pageFiles {
		fileName := filepath.Base(file)
		extension := filepath.Ext(fileName)
		name := fileName[0 : len(fileName)-len(extension)]
		if _, ok := templates[name]; ok {
			return fmt.Errorf("Template %s is already defined by a view, pages and views cannot have the same name", name)
		}
		templates[name] = template.Must(template.ParseFiles(file))
	}
	return nil
}

type Result interface {
	Template() string
	Content() interface{}
	Standalone() bool
}

type Error struct {
	err error
}

func ErrorRenderer(err error) Error {
	return Error{err}
}

func (e Error) Template() string {
	return "server_error"
}
func (e Error) Content() interface{} {
	return e.err
}
func (e Error) Standalone() bool {
	return false
}

type Default struct {
	template   string
	content    interface{}
	standalone bool
}

func DefaultRenderer(template string, content interface{}) Default {
	return Default{template, content, false}
}
func StandaloneRenderer(template string, content interface{}) Default {
	return Default{template, content, true}
}

func (e Default) Template() string {
	return e.template
}
func (e Default) Content() interface{} {
	return e.content
}
func (e Default) Standalone() bool {
	return e.standalone
}

type ClusterInfo struct {
	Hostname string
}

type TemplateData struct {
	User        m.SessionUser
	Cluster     ClusterInfo
	QueryParams map[string]string
	Content     interface{}
	Flashes     map[string][]string
}

func flashes(r *http.Request, w http.ResponseWriter) map[string][]string {
	session, err := cookie.Cookiestore.Get(r, "session")
	if err != nil {
		log.Error("cookie_flashes", log.ErrorEntry{err})
		return nil
	}
	res := make(map[string][]string)
	for k, v := range session.Values {
		strK := k.(string)
		if strings.HasPrefix(strK, "flash_") {
			strK = strK[6:] // strip "flash_" from key
			if _, ok := res[strK]; !ok {
				res[strK] = make([]string, 0)
			}
			for _, vv := range v.([]interface{}) {
				res[strK] = append(res[strK], vv.(string))
			}
			delete(session.Values, k)
		}
	}
	if err = session.Save(r, w); err != nil {
		log.Error("cookie_flashes", log.ErrorEntry{err})
	}
	return res
}

func TemplateHandler(fn func(w http.ResponseWriter, r *http.Request) Result) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		user, ok := r.Context().Value("user").(m.SessionUser)
		if !ok {
			user = m.AnonSessionUser
		}
		hostname, err := os.Hostname()
		if err != nil {
			hostname = ""
		}
		qp := make(map[string]string)
		for k, v := range r.URL.Query() {
			if len(v) > 0 {
				qp[k] = v[0]
			}
		}
		res := fn(w, r)
		if res != nil {
			data := TemplateData{
				User:        user,
				Cluster:     ClusterInfo{hostname},
				QueryParams: qp,
				Content:     res.Content(),
				Flashes:     flashes(r, w),
			}
			err = RenderDefault(w, res.Standalone(), res.Template(), data)
			if err != nil {
				log.Error("render_template", log.ErrorEntry{err})
				w.Write([]byte(err.Error()))
			}
		}
	})
}

func JsonHandler(fn func(r *http.Request) (interface{}, error)) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		res, err := fn(r)
		if err != nil {
			log.Error("web.JsonHandler", log.ErrorEntry{err})
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(res)
	})
}

func SseHandler(fn func(*http.Request, <-chan bool) (<-chan []byte, error)) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		flusher, ok := w.(http.Flusher)
		if !ok {
			http.Error(w, "Streaming unsupported!", http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "text/event-stream")
		w.Header().Set("Cache-Control", "no-cache")
		w.Header().Set("Connection", "keep-alive")
		w.Header().Set("Access-Control-Allow-Origin", "*")

		// Listen to connection close and un-register messageChan
		notify := w.(http.CloseNotifier).CloseNotify()
		quit := make(chan bool, 0)
		resp, err := fn(r, quit)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		for {
			select {
			case <-notify:
				quit <- true
				return
			case v := <-resp:
				fmt.Fprintf(w, "data: %s\n\n", v)
				flusher.Flush()
			}
		}
	})
}

func RenderDefault(w http.ResponseWriter, standalone bool, name string, data interface{}) error {
	if config.DevMode {
		if err := Load(); err != nil {
			return err
		}
	}
	tmpl, ok := templates[name]
	if !ok {
		return fmt.Errorf("The template %s does not exist.", name)
	}
	var err error
	if standalone {
		err = tmpl.Execute(w, data)
	} else {
		err = tmpl.ExecuteTemplate(w, "base", data)
	}
	if err != nil {
		return fmt.Errorf("Template execution failed: %s", err)
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	return nil
}

func RenderError(w http.ResponseWriter, err error) error {
	return RenderDefault(w, false, "server_error", err)
}
