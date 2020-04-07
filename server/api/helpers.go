package api

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strconv"
)

func writeAsJson(w http.ResponseWriter, bytes interface{}) {
	w.Header().Set("Content-Type", "application/json")
	if m, ok := bytes.(json.Marshaler); ok {
		j, _ := m.MarshalJSON()
		fmt.Fprintf(w, string(j))
	} else {
		json.NewEncoder(w).Encode(bytes)
	}
}

func jsonError(w http.ResponseWriter, msg string) {
	w.WriteHeader(http.StatusBadRequest)
	writeAsJson(w, map[string]string{"reason": msg})
}

func parseRequestBody(r *http.Request, target interface{}) error {
	switch r.Header.Get("content-type") {
	case "application/json":
		decoder := json.NewDecoder(r.Body)
		defer r.Body.Close()
		if err := decoder.Decode(target); err != nil {
			return errors.New("Could not parse request body")
		}
		return nil
	}
	return errors.New("This endpoint only supports content type application/json")
}

func pageInfo(r *http.Request) (int, int, error) {
	var (
		p   int
		ps  int
		err error
	)
	page := r.URL.Query().Get("page")
	p, err = strconv.Atoi(page)
	if err != nil {
		err = fmt.Errorf("page must be a number")
	}
	pageSize := r.URL.Query().Get("page_size")
	ps, err = strconv.Atoi(pageSize)
	if err != nil {
		err = fmt.Errorf("page must be a number")
	}
	return ps, p, err
}

type page struct {
	PageSize      int           `json:"page_size"`
	Page          int           `json:"page"`
	ItemCount     int           `json:"item_count"`
	TotalCount    int           `json:"total_count"`
	Items         []interface{} `json:"items"`
	FilteredCount int           `json:"filtered_count"`
}

type pageable interface {
	Get(int) interface{}
	Size() int
}

func Page(pageSize, p int, items pageable) page {
	var (
		result = make([]interface{}, 0, pageSize)
		tc     = items.Size()
	)
	for i := (p - 1) * pageSize; i < p*pageSize; i++ {
		if i >= tc {
			break
		}
		result = append(result, items.Get(i))
	}
	return page{pageSize, p, len(result), tc, result, tc}
}

func SSEHeaders(rw http.ResponseWriter) {
	rw.Header().Set("Content-Type", "text/event-stream")
	rw.Header().Set("Cache-Control", "no-cache")
	rw.Header().Set("Connection", "keep-alive")
	rw.Header().Set("Access-Control-Allow-Origin", "*")
	rw.Header().Set("X-Accel-Buffering", "no")
}

func SetupSSE(retry int, rw http.ResponseWriter) {
	SSEHeaders(rw)
	fmt.Fprint(rw, "\n")
	fmt.Fprint(rw, ":\n")
	fmt.Fprint(rw, fmt.Sprintf("retry: %v\n\n", retry))
}
