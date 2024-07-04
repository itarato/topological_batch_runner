# Parallel Topological Batch Runner

Topological batch runner is a tiny library that allows efficient parallel execution of topologically ordered operations.
Simple topological ordering provides a linear list, which is not trivial to parallelize. See the following example.

We have 3 operations, A, B and C, where B and C depends on A (meaning A needs to come first, then B or C in arbitrary
order). A linear execution order would be A then B then C or A then C then B. However once A is computed, B and C can
be executed parallel. This is what this library is for.

A more practical example:

```rust
/// The structure responsible running the dependent operations. Must be Send and Sync.
struct ExecutorExample {
    /// Inner data.
}

/// The implementation of the execution. ID represents the link to the topological structure.
impl CallableByID<usize> for ExecutorExample {
    fn call(&self, id: usize) {
        /// Code to execute parallel - for an ID that came after all of its dependencies.
    }
}

/// Next setup the dependency graph:
let mut dependency_graph: HashMap<usize, Vec<usize>> = HashMap::new();
dependency_graph.insert(0, vec![4, 2, 5]);
/// ...

/// Initialize executor and run:
let topological_batch_provider = TopologicalBatchProvider::new(dependency_graph.clone())?;
let runner = ThreadPoolRunner::new(8);
let executor = Arc::new(ExecutorExample {});
runner.run(topological_batch_provider, executor);
```

The topological ordering is defined with IDs, that act as a pointer to computation units. An ID should be
as light as possible (eg `usize`) to be efficiently worked with.
