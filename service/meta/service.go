package meta

import (
	"context"
	"fmt"
	"github.com/viant/afs"
	"github.com/viant/afs/file"
	"github.com/viant/afs/storage"
	"github.com/viant/afs/url"
	"gopkg.in/yaml.v3"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

// Service represents a YAML loader that can resolve $import directives.
type Service struct {
	fs      afs.Service
	baseURL string
	options []storage.Option
}

// Load reads the YAML file at the given path, resolves $import directives,
// and decodes the result into the provided Go variable v.
func (l *Service) Load(ctx context.Context, path string, v interface{}) error {
	URL := l.getURL(path)
	return l.LoadWithURL(ctx, URL, v)
}

func (l *Service) Exists(ctx context.Context, path string) (bool, error) {
	URL := l.getURL(path)

	return l.fs.Exists(ctx, URL, l.options...)
}

func (l *Service) List(ctx context.Context, path string) ([]string, error) {
	var result []string
	URL := l.getURL(path)
	objects, err := l.fs.List(ctx, URL, l.options...)
	if err != nil {
		return nil, err
	}
	for _, object := range objects {
		if object.IsDir() {
			continue
		}
		result = append(result, object.URL())
	}
	return result, nil
}

func (l *Service) Download(ctx context.Context, path string) ([]byte, error) {
	URL := l.getURL(path)
	return l.fs.DownloadWithURL(ctx, URL, l.options...)
}

// resolveDependencies recursively resolves $import directives within a YAML node.
func (l *Service) resolveDependencies(ctx context.Context, node *yaml.Node, baseDir string) error {
	switch node.Kind {
	case yaml.DocumentNode:
		// Recursively resolve imports in document content.
		for _, contentNode := range node.Content {
			if err := l.resolveDependencies(ctx, contentNode, baseDir); err != nil {
				return err
			}
		}
	case yaml.MappingNode:
		// Mapping nodes have content in key-value pairs.
		for i := 0; i < len(node.Content); i += 2 {
			keyNode := node.Content[i]
			valueNode := node.Content[i+1]

			// Resolve imports in the key and value nodes.
			if err := l.processNode(ctx, keyNode, baseDir); err != nil {
				return err
			}
			if err := l.processNode(ctx, valueNode, baseDir); err != nil {
				return err
			}
		}
	case yaml.SequenceNode:
		// Resolve imports in each item of the sequence.
		for i := 0; i < len(node.Content); i++ {
			itemNode := node.Content[i]
			if err := l.processNode(ctx, itemNode, baseDir); err != nil {
				return err
			}
		}
	case yaml.AliasNode:
		// Resolve imports in the referenced node.
		if node.Alias != nil {
			if err := l.resolveDependencies(ctx, node.Alias, baseDir); err != nil {
				return err
			}
		}
	default:
		// ScalarNode or others; do nothing here.
	}
	return nil
}

// getImportPathAndKey extracts the file path and an optional key from an import directive.
// It accepts YAML (.yaml or .yml) and text (.txt) files. If no extension is provided, it defaults to .yaml.
func (l *Service) getImportPathAndKey(ctx context.Context, value string) (path string, key string, err error) {
	value = strings.TrimSpace(value)
	if !isImportDirective(value) {
		return "", "", fmt.Errorf("not an import directive: %s", value)
	}
	// Extract the path inside the parentheses.
	start := strings.Index(value, "(")
	end := strings.LastIndex(value, ")")
	if start == -1 || end == -1 || start >= end {
		return "", "", fmt.Errorf("invalid import directive syntax: %s", value)
	}
	pathValue := strings.TrimSpace(value[start+1 : end])
	// Remove surrounding quotes if present.
	pathValue = strings.Trim(pathValue, "\"'")

	// Check if a key is specified after a colon.
	parts := strings.SplitN(pathValue, ":", 2)
	path = parts[0]
	if len(parts) > 1 {
		key = parts[1]
	}

	// Determine file extension.
	ext := filepath.Ext(path)
	if ext == "" {
		if ok, _ := l.fs.Exists(ctx, path+".txt", l.options...); ok {
			path += ".txt"
		} else { //default yaml
			path += ".yaml"
		}
	} else if ext != ".yaml" && ext != ".yml" && ext != ".txt" && ext != ".vm" {
		return "", "", fmt.Errorf("unsupported file extension: %s", ext)
	}
	return path, key, nil
}

// processNode checks if the node contains an $import directive and processes it.
// It resolves the file path, downloads the file, and then either unmarshals YAML content or loads plain text.
func (l *Service) processNode(ctx context.Context, node *yaml.Node, baseDir string) error {
	if node.Kind == yaml.ScalarNode && node.Tag == "!!str" {

		if isPathDirective(node.Value) {
			importPath, _, err := l.getImportPathAndKey(ctx, node.Value)
			if err != nil {
				return err
			}
			node.Value = importPath
			return nil
		}
		if isImportDirective(node.Value) {
			importPath, key, err := l.getImportPathAndKey(ctx, node.Value)
			if err != nil {
				return err
			}
			// Resolve the full path of the imported file.
			fullPath := url.Join(baseDir, importPath)
			// Read the file from the provided URL.
			data, err := l.fs.DownloadWithURL(ctx, fullPath, l.options...)
			if err != nil {
				return err
			}

			var importedNode yaml.Node
			ext := filepath.Ext(importPath)
			switch strings.ToLower(ext) {
			case ".yaml", ".yml":
				if err := yaml.Unmarshal(data, &importedNode); err != nil {
					return err
				}
			default:
				importedNode = yaml.Node{
					Kind:  yaml.ScalarNode,
					Tag:   "!!str",
					Value: string(data),
				}
			}
			// Determine the parent directory for resolving nested imports.
			parent, _ := url.Split(fullPath, file.Scheme)
			// Resolve imports in the imported node recursively.
			if err := l.resolveDependencies(ctx, &importedNode, parent); err != nil {
				return err
			}

			var replacementNode *yaml.Node
			if key == "" {
				// No specific key requested; use the entire imported content.
				replacementNode = getContentNode(&importedNode)
			} else {
				// Extract the node under the specified key.
				extractedNode, err := getNodeByKey(&importedNode, key)
				if err != nil {
					return err
				}
				replacementNode = extractedNode
			}

			// Replace the current node with the imported content or the extracted node.
			*node = *replacementNode
		}
	} else {
		// Recursively resolve imports in this node.
		if err := l.resolveDependencies(ctx, node, baseDir); err != nil {
			return err
		}
	}
	return nil
}

// isImportDirective checks if a string is an $import directive.
func isImportDirective(value string) bool {
	return strings.HasPrefix(strings.TrimSpace(value), "$import")
}

// isImportDirective checks if a string is an $import directive.
func isPathDirective(value string) bool {
	return strings.HasPrefix(strings.TrimSpace(value), "$path")
}

// getNodeByKey navigates through the YAML node to find the node under the specified key path.
func getNodeByKey(node *yaml.Node, key string) (*yaml.Node, error) {
	keys := strings.Split(key, ".")
	currentNode := node

	for _, k := range keys {
		if currentNode.Kind == yaml.DocumentNode {
			if len(currentNode.Content) > 0 {
				currentNode = currentNode.Content[0]
			} else {
				return nil, fmt.Errorf("document node has no content")
			}
		}

		if currentNode.Kind == yaml.MappingNode {
			found := false
			for i := 0; i < len(currentNode.Content); i += 2 {
				keyNode := currentNode.Content[i]
				valueNode := currentNode.Content[i+1]
				if keyNode.Value == k {
					currentNode = valueNode
					found = true
					break
				}
			}
			if !found {
				return nil, fmt.Errorf("key '%s' not found in mapping", k)
			}
		} else if currentNode.Kind == yaml.SequenceNode {
			// Allow numeric keys for sequences.
			index, err := parseIndex(k)
			if err != nil {
				return nil, fmt.Errorf("invalid sequence index '%s': %v", k, err)
			}
			if index < 0 || index >= len(currentNode.Content) {
				return nil, fmt.Errorf("sequence index '%d' out of range", index)
			}
			currentNode = currentNode.Content[index]
		} else {
			return nil, fmt.Errorf("cannot navigate through node of kind %d", currentNode.Kind)
		}
	}

	return currentNode, nil
}

func (l *Service) LoadWithURL(ctx context.Context, URL string, v interface{}) error {
	// Read the file content using the filesystem service.
	data, err := l.fs.DownloadWithURL(ctx, URL, l.options...)
	if err != nil {
		return err
	}
	object, _ := l.fs.Object(ctx, URL, l.options)

	// Parse the YAML into a yaml.Node.
	var node yaml.Node
	if err := yaml.Unmarshal(data, &node); err != nil {
		return err
	}

	// Resolve $import directives recursively.
	baseDir, _ := url.Split(object.URL(), file.Scheme)

	if err := l.resolveDependencies(ctx, &node, baseDir); err != nil {
		return err
	}

	// Decode the resolved YAML node into the provided Go variable.
	err = node.Decode(v)

	return err
}

func (l *Service) getURL(path string) string {
	URL := path
	if url.Scheme(URL, "") != "" || !url.IsRelative(path) {
		return URL
	}
	if l.baseURL != "" {
		URL = url.Join(l.baseURL, path)
	}
	if url.IsRelative(URL) {
		if basePath, _ := os.Getwd(); basePath != "" {
			URL = filepath.Join(basePath, path)
		}
	}
	return URL
}

// parseIndex converts a string to an integer index.
func parseIndex(s string) (int, error) {
	return strconv.Atoi(s)
}

// getContentNode extracts the content node from a YAML node,
// handling DocumentNode and other cases.
func getContentNode(node *yaml.Node) *yaml.Node {
	if node.Kind == yaml.DocumentNode && len(node.Content) > 0 {
		return node.Content[0]
	}
	return node
}

// New creates a new Service with the provided filesystem service.
func New(fs afs.Service, baseURL string, options ...storage.Option) *Service {
	return &Service{fs: fs, baseURL: baseURL, options: options}
}
