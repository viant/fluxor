package storage

import "time"

// Asset represents a file or directory in storage
type Asset struct {
	URL         string    `json:"url"`
	Name        string    `json:"Name"`
	IsDir       bool      `json:"isDir"`
	Mode        string    `json:"mode,omitempty"`
	Size        int64     `json:"size,omitempty"`
	ModTime     time.Time `json:"modTime,omitempty"`
	Data        []byte    `json:"data,omitempty"`
	ContentType string    `json:"contentType,omitempty"`
}
