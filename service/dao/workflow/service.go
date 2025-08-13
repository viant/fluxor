package workflow

import (
	"context"
	"fmt"
	"github.com/viant/afs"
	"github.com/viant/fluxor/model"
	"github.com/viant/fluxor/model/graph"
	"github.com/viant/fluxor/model/state"
	"github.com/viant/fluxor/service/dao/workflow/parameters"
	"github.com/viant/fluxor/service/meta"
	"github.com/viant/fluxor/service/meta/yml"
	"gopkg.in/yaml.v3"
	"path/filepath"
	"strings"
	"sync"
)

type Service struct {
	metaService      *meta.Service
	rootTaskNodeName string
	cache            map[string]*model.Workflow
	mu               sync.RWMutex
}

// RootTaskNodeName returns the root task node name
func (s *Service) RootTaskNodeName() string {
	return s.rootTaskNodeName
}

// canonicalizeLocation ensures the provided location has a supported extension.
// If no extension is specified, ".yaml" is appended to be consistent with the
// Load behaviour and cache keys.
func canonicalizeLocation(location string) string {
	if ext := filepath.Ext(location); ext == "" {
		return location + ".yaml"
	}
	return location
}

// getFromCache returns a cached workflow (if present) for the given location.
func (s *Service) getFromCache(location string) (*model.Workflow, bool) {
	if s == nil {
		return nil, false
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	wf, ok := s.cache[location]
	return wf, ok
}

// storeToCache stores a workflow in the cache under the provided location.
func (s *Service) storeToCache(location string, wf *model.Workflow) {
	if s == nil || wf == nil {
		return
	}
	s.mu.Lock()
	if s.cache == nil {
		s.cache = make(map[string]*model.Workflow)
	}
	s.cache[location] = wf
	s.mu.Unlock()
}

// refresh removes the cached workflow under the provided location, if any.
func (s *Service) refresh(location string) {
	s.mu.Lock()
	if s.cache != nil {
		delete(s.cache, location)
	}
	s.mu.Unlock()
}

// Refresh discards any cached copy of the workflow definition identified by
// the provided location. The next Load call will reload the file via the
// meta-service.
func (s *Service) Refresh(location string) {
	location = canonicalizeLocation(location)
	s.refresh(location)
}

// Upsert stores the supplied workflow definition in the cache under the given
// location, replacing any existing entry.
func (s *Service) Upsert(location string, wf *model.Workflow) {
	location = canonicalizeLocation(location)
	// Ensure the Source URL is aligned with the provided location.
	if wf != nil {
		if wf.Source == nil {
			wf.Source = &model.Source{URL: location}
		} else {
			wf.Source.URL = location
		}
	}
	s.storeToCache(location, wf)
}

// DecodeYAML decodes a workflow from YAML
func (s *Service) DecodeYAML(encoded []byte) (*model.Workflow, error) {
	var node yaml.Node
	if err := yaml.Unmarshal(encoded, &node); err != nil {
		return nil, err
	}
	return s.ParseWorkflow("", &node)
}

// Load loads a workflow from YAML at the specified Location
func (s *Service) Load(ctx context.Context, URL string) (*model.Workflow, error) {
	// Normalise location first so that caching keys are consistent.
	URL = canonicalizeLocation(URL)

	// Return cached copy if available.
	if wf, ok := s.getFromCache(URL); ok {
		return wf, nil
	}

	// Not cached – read from underlying storage.
	var node yaml.Node
	if err := s.metaService.Load(ctx, URL, &node); err != nil {
		return nil, fmt.Errorf("failed to load workflow from %s: %w", URL, err)
	}

	wf, err := s.ParseWorkflow(URL, &node)
	if err != nil {
		return nil, err
	}

	// Store parsed workflow in cache for future use.
	s.storeToCache(URL, wf)

	return wf, nil
}

func (s *Service) ParseWorkflow(URL string, node *yaml.Node) (*model.Workflow, error) {
	workflow := &model.Workflow{
		Source: &model.Source{
			URL: URL,
		},
		Name: getWorkflowNameFromURL(URL),
	}

	// Parse the YAML into our workflow model
	if err := s.parseWorkflow((*yml.Node)(node), workflow); err != nil {
		return nil, fmt.Errorf("failed to parse workflow from %s: %w", URL, err)
	}

	// Set name based on Location if not set
	if workflowName := workflow.Name; workflowName == "" {
		workflow.Name = generateAnonymousName()
	}

	// Process tasks to assign IDs
	if workflow.Pipeline != nil {
		assignTaskIDs(workflow.Pipeline, workflow.Name, "")
	}

	if issues := workflow.Validate(); len(issues) > 0 {
		return nil, issues[0]
	}
	return workflow, nil
}

// getWorkflowNameFromURL extracts workflow name from Location (file name without extension)
func getWorkflowNameFromURL(URL string) string {
	base := filepath.Base(URL)
	return strings.TrimSuffix(base, filepath.Ext(base))
}

// assignTaskIDs recursively assigns IDs to tasks based on workflow name
func assignTaskIDs(task *graph.Task, workflowName, parentID string) {
	// Set root task ID to workflow name if it's empty
	if task.ID == "" && parentID == "" {
		task.ID = workflowName
	}

	// Set namespace to task name if empty
	if task.Namespace == "" && task.Name != "" {
		task.Namespace = task.Name
	}

	taskID := task.ID
	if parentID != "" {
		taskID = parentID + "/" + taskID
	}

	// Update task ID to include parent path
	task.ID = taskID

	// Process subtasks
	for _, subtask := range task.Tasks {
		assignTaskIDs(subtask, workflowName, taskID)
	}
}

// parseWorkflow converts YAML node to workflow model
func (s *Service) parseWorkflow(node *yml.Node, workflow *model.Workflow) error {
	rootNode := node
	if node.Kind == yaml.DocumentNode && len(node.Content) > 0 {
		rootNode = (*yml.Node)(node.Content[0])
	}
	rootNodeName := strings.ToLower(s.rootTaskNodeName)
	// Parse workflow properties
	err := rootNode.Pairs(func(key string, valueNode *yml.Node) error {
		lowerKey := strings.ToLower(key)
		switch lowerKey {
		case "name":
			if valueNode.Kind == yaml.ScalarNode {
				workflow.Name = valueNode.Value
			}
		case "typename":
			if valueNode.Kind == yaml.ScalarNode {
				workflow.TypeName = valueNode.Value
			}
		case "import":
			workflow.Imports = make([]*model.Import, 0)
			if valueNode.Kind == yaml.MappingNode {
				if err := valueNode.Pairs(func(importKey string, importValue *yml.Node) error {
					workflow.Imports = append(workflow.Imports, &model.Import{
						Package: importKey,
						PkgPath: importValue.Value,
					})
					return nil
				}); err != nil {
					return fmt.Errorf("failed to parse import: %w", err)
				}
				workflow.TypeName = valueNode.Value
			}
		case "autopause":
			flag, ok := valueNode.Interface().(bool)
			if !ok {
				return fmt.Errorf("autopause should be a boolean")
			}
			workflow.AutoPause = &flag

		case "init":
			init, err := parseParameters(valueNode)
			if err != nil {
				return fmt.Errorf("failed to parse post parameters: %w", err)
			}
			workflow.Init = init
		case "post":
			post, err := parseParameters(valueNode)
			if err != nil {
				return fmt.Errorf("failed to parse post parameters: %w", err)
			}
			workflow.Post = post
		case rootNodeName:
			// The pipeline node supports two YAML forms:
			//  1. Direct mapping/sequence of tasks.
			//       pipeline:
			//         taskA: { ... }
			//  2. A wrapper object carrying additional metadata such as `kind` plus
			//     the actual tasks under a nested `tasks:` field.
			//       pipeline:
			//         kind: Sequence
			//         tasks: [ ... ]
			//     Historically only the first form was supported which led to
			//     mis-parsing of the second, causing errors like
			//       "task node should be a mapping".
			//     Below we transparently unwrap the second representation so that the
			//     existing task parsing logic can operate unchanged.
			if valueNode != nil && valueNode.Kind == yaml.MappingNode {
				if tasksField := valueNode.Lookup("tasks"); tasksField != nil {
					valueNode = tasksField
				}
			}

			pipeline, err := s.parseRootTask(valueNode)
			if err != nil {
				return fmt.Errorf("failed to parse pipeline: %w", err)
			}
			workflow.Pipeline = pipeline

		case "dependencies":
			workflow.Dependencies = make(map[string]*graph.Task)
			deps, err := s.parseRootTask(valueNode)
			if err != nil {
				return fmt.Errorf("failed to parse pipeline: %w", err)
			}
			for i := range deps.Tasks {
				dep := deps.Tasks[i]
				workflow.Dependencies[dep.Name] = dep
			}
		}

		return nil
	})
	return err
}

// parseRootTask converts YAML node to graph.Task
func (s *Service) parseRootTask(node *yml.Node) (*graph.Task, error) {
	if node == nil {
		return nil, fmt.Errorf("pipeline node should be a mapping or sequence")
	}
	pipelineTask := &graph.Task{}
	var tasks []*graph.Task
	switch node.Kind {
	case yaml.MappingNode:
		if err := node.Pairs(func(key string, taskNode *yml.Node) error {
			task, err := s.parseTask(key, taskNode)
			if err != nil {
				return err
			}
			tasks = append(tasks, task)
			return nil
		}); err != nil {
			return nil, err
		}
	case yaml.SequenceNode:
		for _, item := range node.Content {
			if item.Kind != yaml.MappingNode {
				return nil, fmt.Errorf("pipeline sequence items should be mappings")
			}
			mapping := (*yml.Node)(item)
			if idNode := mapping.Lookup("id"); idNode != nil {
				// -------------------------------------------------------
				// Standard form where the task definition carries an
				// explicit `id:` attribute.
				// -------------------------------------------------------
				id := idNode.Value
				task, err := s.parseTask(id, mapping)
				if err != nil {
					return nil, err
				}
				tasks = append(tasks, task)
				continue
			}

			// -----------------------------------------------------------------
			// Alternate compact form ‑ the YAML item is a single-entry mapping
			// where the key is the task ID and the value is the task body.  This
			// mirrors the mapping style allowed at the root `pipeline:` node but
			// nested inside a sequence so that the execution order is preserved.
			// Example:
			//   - list:
			//       service: system/storage
			//       action: list
			// -----------------------------------------------------------------
			if len(mapping.Content)%2 == 0 && len(mapping.Content) >= 2 {
				for i := 0; i < len(mapping.Content); i += 2 {
					keyNode := mapping.Content[i]
					valueNode := (*yml.Node)(mapping.Content[i+1])

					if keyNode == nil || keyNode.Kind != yaml.ScalarNode {
						return nil, fmt.Errorf("invalid task mapping in sequence item; task name must be a scalar key")
					}

					id := keyNode.Value
					task, err := s.parseTask(id, valueNode)
					if err != nil {
						return nil, err
					}
					tasks = append(tasks, task)
				}
			} else {
				return nil, fmt.Errorf("task id is required for sequence pipeline item")
			}
		}
	default:
		return nil, fmt.Errorf("pipeline node should be a mapping or sequence")
	}
	pipelineTask.Tasks = tasks
	return pipelineTask, nil
}

// parseTask converts a YAML node to a graph.Task
func (s *Service) parseTask(id string, node *yml.Node) (*graph.Task, error) {
	task := &graph.Task{
		ID:   id,
		Name: id,
	}

	if node.Kind != yaml.MappingNode {
		return nil, fmt.Errorf("task node should be a mapping")
	}

	// Parse task properties
	err := node.Pairs(func(key string, valueNode *yml.Node) error {
		lowerKey := strings.ToLower(key)
		switch lowerKey {
		case "id":
			if valueNode.Kind == yaml.ScalarNode {
				task.ID = valueNode.Value
				task.Name = valueNode.Value
			}
			return nil
		case "service":
			if valueNode.Kind == yaml.ScalarNode {
				if task.Action == nil {
					task.Action = &graph.Action{}
				}
				task.Action.Service = valueNode.Value
			}
			return nil
		case "action":
			if valueNode.Kind == yaml.ScalarNode {
				parts := strings.Split(valueNode.Value, ":")
				action := &graph.Action{}
				if len(parts) > 1 {
					action.Service = parts[0]
					action.Method = parts[1]
				} else if task.Action != nil && task.Action.Service != "" {
					action.Service = task.Action.Service
					action.Method = parts[0]
				} else {
					action.Service = parts[0]
				}
				task.Action = action
			} else if valueNode.Kind == yaml.MappingNode {
				action := &graph.Action{}
				_ = valueNode.Pairs(func(actionKey string, actionValue *yml.Node) error {
					actionKeyLower := strings.ToLower(actionKey)
					switch actionKeyLower {
					case "service":
						action.Service = actionValue.Value
					case "method":
						action.Method = actionValue.Value
					case "input":
						action.Input = actionValue.Interface()
					}
					return nil
				})
				task.Action = action
			}
		case "async":
			flag, ok := valueNode.Interface().(bool)
			if !ok {
				return fmt.Errorf("async should be a boolean")
			}
			task.Async = flag
		case "init":
			params, err := parseParameters(valueNode)
			if err != nil {
				return err
			}
			task.Init = params
		case "post":
			params, err := parseParameters(valueNode)
			if err != nil {
				return err
			}
			task.Post = params
		case "when":
			if valueNode.Kind == yaml.ScalarNode {
				task.When = valueNode.Value
			}
		case "name":
			if valueNode.Kind == yaml.ScalarNode {
				task.Name = valueNode.Value
			}
		case "typename":
			if valueNode.Kind == yaml.ScalarNode {
				task.TypeName = valueNode.Value
			}
		case "autopause":
			flag, ok := valueNode.Interface().(bool)
			if !ok {
				return fmt.Errorf("autopause should be a boolean")
			}
			task.AutoPause = &flag

		case "namespace":
			if valueNode.Kind == yaml.ScalarNode {
				task.Namespace = valueNode.Value
			}
		case "dependson":
			switch valueNode.Kind {
			case yaml.SequenceNode:
				slice, ok := valueNode.Interface().([]string)
				if !ok {
					return fmt.Errorf("dependson should be a string or a slice of strings")
				}
				task.DependsOn = slice

			case yaml.ScalarNode:
				text, ok := valueNode.Interface().(string)
				if !ok {
					return fmt.Errorf("dependson should be a string or a slice of strings")
				}
				task.DependsOn = []string{text}
			}
		case "goto":
			if valueNode.Kind == yaml.SequenceNode {
				for _, transNode := range valueNode.Content {
					trans, err := parseTransition((*yml.Node)(transNode))
					if err != nil {
						return err
					}
					task.Goto = append(task.Goto, trans)
				}
			} else if valueNode.Kind == yaml.MappingNode {
				trans, err := parseTransition(valueNode)
				if err != nil {
					return err
				}
				task.Goto = append(task.Goto, trans)
			}
		case "input":
			if task.Action == nil {
				task.Action = &graph.Action{}
			}
			task.Action.Input = valueNode.Interface()
		case "with":
			if task.Action == nil {
				task.Action = &graph.Action{}
			}
			task.Action.Input = valueNode.Interface()
		case "emit":
			emitSpec, err := s.parseEmitSpec(valueNode)
			if err != nil {
				return err
			}
			task.Emit = emitSpec
		case "await":
			awaitSpec, err := s.parseAwaitSpec(valueNode)
			if err != nil {
				return err
			}
			task.Await = awaitSpec

		// Handle template definitions: repeat a sub-task over a collection
		case "template":
			if valueNode.Kind == yaml.MappingNode {
				tmpl := &graph.Template{}
				// parse selector and inner task
				if err := valueNode.Pairs(func(innerKey string, innerNode *yml.Node) error {
					switch strings.ToLower(innerKey) {
					case "selector":
						params, err := parseSelector(innerNode)
						if err != nil {
							return err
						}
						tmpl.Selector = &params
					case "task":
						if innerNode.Kind == yaml.MappingNode {
							// expect a single child defining the task id
							return innerNode.Pairs(func(taskKey string, taskNode *yml.Node) error {
								child, err := s.parseTask(taskKey, taskNode)
								if err != nil {
									return err
								}
								tmpl.Task = child
								return nil
							})
						}
					}
					return nil
				}); err != nil {
					return fmt.Errorf("failed to parse template for task %s: %w", id, err)
				}
				task.Template = tmpl
			}
		default:
			// It could be a sub-task if the value is a mapping
			if valueNode.Kind == yaml.MappingNode {
				subTask, err := s.parseTask(key, valueNode)
				if err != nil {
					return err
				}
				task.Tasks = append(task.Tasks, subTask)
			}
		}
		return nil
	})
	if task.Namespace == "" {
		task.Namespace = task.Name
	}

	if err != nil {
		return nil, err
	}

	return task, nil
}

// parseTransition converts a YAML node to a graph.Transition
func parseTransition(node *yml.Node) (*graph.Transition, error) {
	if node.Kind != yaml.MappingNode {
		return nil, fmt.Errorf("transition node should be a mapping")
	}

	transition := &graph.Transition{}

	err := node.Pairs(func(key string, valueNode *yml.Node) error {
		lowerKey := strings.ToLower(key)
		switch lowerKey {
		case "when":
			if valueNode.Kind == yaml.ScalarNode {
				transition.When = valueNode.Value
			}
		case "task":
			if valueNode.Kind == yaml.ScalarNode {
				transition.Task = valueNode.Value
			}
		}
		return nil
	})

	if err != nil {
		return nil, err
	}

	return transition, nil
}

// parseEmitSpec converts YAML node to graph.EmitSpec
func (s *Service) parseEmitSpec(node *yml.Node) (*graph.EmitSpec, error) {
	if node == nil {
		return nil, fmt.Errorf("emit spec should be a mapping")
	}
	// Allow boolean false/true for backward compatibility – treat as nil or default behaviour
	if node.Kind == yaml.ScalarNode {
		// e.g., emit: false
		if val, ok := node.Interface().(bool); ok && !val {
			return nil, nil
		}
		return nil, fmt.Errorf("emit spec must be a mapping, got scalar")
	}
	if node.Kind != yaml.MappingNode {
		return nil, fmt.Errorf("emit spec should be a mapping")
	}

	spec := &graph.EmitSpec{}

	err := node.Pairs(func(key string, valueNode *yml.Node) error {
		switch strings.ToLower(key) {
		case "foreach":
			if valueNode.Kind == yaml.ScalarNode {
				spec.ForEach = valueNode.Value
			}
		case "as":
			if valueNode.Kind == yaml.ScalarNode {
				spec.As = valueNode.Value
			}
		case "task":
			// Expect mapping with a single top-level key (task id)
			if valueNode.Kind != yaml.MappingNode {
				return fmt.Errorf("emit.task should be a mapping")
			}
			return valueNode.Pairs(func(id string, taskNode *yml.Node) error {
				task, err := s.parseTask(id, taskNode)
				if err != nil {
					return err
				}
				spec.Task = task
				return nil
			})
		}
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("failed to parse emit spec: %w", err)
	}
	return spec, nil
}

// parseAwaitSpec converts YAML node to graph.AwaitSpec
func (s *Service) parseAwaitSpec(node *yml.Node) (*graph.AwaitSpec, error) {
	if node == nil {
		return nil, fmt.Errorf("await spec is nil")
	}
	// If scalar boolean
	if node.Kind == yaml.ScalarNode {
		if val, ok := node.Interface().(bool); ok {
			if !val {
				return nil, nil // await: false – no waiting
			}
			// await: true – use default spec
			return &graph.AwaitSpec{}, nil
		}
	}

	if node.Kind != yaml.MappingNode {
		return nil, fmt.Errorf("await spec should be a mapping or bool")
	}
	spec := &graph.AwaitSpec{}

	err := node.Pairs(func(key string, valueNode *yml.Node) error {
		switch strings.ToLower(key) {
		case "mode", "on":
			if valueNode.Kind == yaml.ScalarNode {
				if strings.ToLower(key) == "mode" {
					spec.Mode = valueNode.Value
				} else {
					spec.Group = valueNode.Value
				}
			}
		case "timeout":
			if valueNode.Kind == yaml.ScalarNode {
				spec.Timeout = valueNode.Value
			}
		case "merge":
			if valueNode.Kind == yaml.ScalarNode {
				spec.Merge = strings.ToLower(valueNode.Value)
			}
		}
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("failed to parse await spec: %w", err)
	}
	return spec, nil
}

// parseParameters converts a YAML node to state.State
func parseParameters(node *yml.Node) (state.Parameters, error) {
	var params state.Parameters

	if node.Kind != yaml.MappingNode {
		return nil, fmt.Errorf("parameters node should be a mapping")
	}

	err := node.Pairs(func(key string, valueNode *yml.Node) error {
		if strings.Contains(key, "[") && !strings.HasSuffix(key, "[]") {
			parameter, err := parameters.Parse([]byte(key))
			if err != nil {
				return fmt.Errorf("failed to parse parameter: %w", err)
			}
			parameter.Value = valueNode.Interface()
			params = append(params, parameter)
			return nil
		}
		val := valueNode.Interface()
		// Test expectations rely on numeric literals to be decoded as float64 so
		// that they match JSON round-trip semantics used in golden files.  The
		// YAML library, however, returns int for untyped numeric scalars.  Cast
		// integers to float64 to achieve stable cross-type comparisons.
		switch typed := val.(type) {
		case int: // handle signed ints; the majority of test fixtures use small ints
			val = float64(typed)
		case int64:
			val = float64(typed)
		case uint:
			val = float64(typed)
		case uint64:
			val = float64(typed)
		}
		params = append(params, &state.Parameter{Name: key, Value: val})
		return nil
	})

	if err != nil {
		return nil, err
	}

	return params, nil
}

// parseSelector parses a sequence of selector parameter mappings into state.Parameters
func parseSelector(node *yml.Node) (state.Parameters, error) {
	var params state.Parameters
	if node.Kind != yaml.SequenceNode {
		return nil, fmt.Errorf("selector node should be a sequence")
	}
	for _, item := range node.Content {
		if item.Kind != yaml.MappingNode {
			return nil, fmt.Errorf("selector items must be mappings")
		}
		var name string
		var value interface{}

		mappingItem := (*yml.Node)(item)
		if err := mappingItem.Pairs(func(key string, valueNode *yml.Node) error {
			switch strings.ToLower(key) {
			case "name":
				name = valueNode.Value
			case "value":
				value = valueNode.Interface()
			}
			return nil
		}); err != nil {
			return nil, err
		}
		params = append(params, &state.Parameter{Name: name, Value: value})
	}
	return params, nil
}

// New creates a new workflow service instance
func New(opts ...Option) *Service {
	ret := &Service{
		metaService:      meta.New(afs.New(), ""),
		rootTaskNodeName: "pipeline",
	}
	for _, opt := range opts {
		opt(ret)
	}
	return ret
}
