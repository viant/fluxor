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
)

type Service struct {
	metaService      *meta.Service
	rootTaskNodeName string
}

// RootTaskNodeName returns the root task node name
func (s *Service) RootTaskNodeName() string {
	return s.rootTaskNodeName
}

// DecodeYAML decodes a workflow from YAML
func (s *Service) DecodeYAML(encoded []byte) (*model.Workflow, error) {
	var node yaml.Node
	if err := yaml.Unmarshal(encoded, &node); err != nil {
		return nil, err
	}
	return s.ParseWorkflow("", &node)
}

// Load loads a workflow from YAML at the specified URL
func (s *Service) Load(ctx context.Context, URL string) (*model.Workflow, error) {
	ext := filepath.Ext(URL)
	if ext == "" {
		URL += ".yaml"
	}
	var node yaml.Node
	if err := s.metaService.Load(ctx, URL, &node); err != nil {
		return nil, fmt.Errorf("failed to load workflow from %s: %w", URL, err)
	}

	return s.ParseWorkflow(URL, &node)
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

	// Set name based on URL if not set
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

// getWorkflowNameFromURL extracts workflow name from URL (file name without extension)
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
	if node == nil || node.Kind != yaml.MappingNode {
		return nil, fmt.Errorf("pipeline node should be a mapping")
	}

	pipelineTask := &graph.Task{}

	// Parse each task in the pipeline
	var tasks []*graph.Task
	err := node.Pairs(func(key string, taskNode *yml.Node) error {
		task, err := s.parseTask(key, taskNode)
		if err != nil {
			return err
		}
		tasks = append(tasks, task)
		return nil
	})

	if err != nil {
		return nil, err
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
		// Case-insensitive matching is handled by yml.Node
		lowerKey := strings.ToLower(key)
		switch lowerKey {
		case "action":
			if valueNode.Kind == yaml.ScalarNode {
				parts := strings.Split(valueNode.Value, ":")
				action := &graph.Action{
					Service: parts[0],
				}
				if len(parts) > 1 {
					action.Method = parts[1]
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
