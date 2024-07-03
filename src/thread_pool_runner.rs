use std::{
    hash::Hash,
    sync::{Arc, Mutex},
    thread,
    time::Duration,
};

use super::common::*;
use super::topological_batch_provider::*;

pub struct ThreadPoolRunner {
    thread_count: usize,
}

impl ThreadPoolRunner {
    pub fn new(thread_count: usize) -> Self {
        Self { thread_count }
    }

    pub fn run<T: Hash + PartialEq + Eq + Clone + Send + 'static>(
        &self,
        topological_batch_provider: TopologicalBatchProvider<T>,
        node_executor: Arc<dyn CallableByID<T> + Send + Sync>,
    ) {
        let provider = Arc::new(Mutex::new(topological_batch_provider));
        let mut handles = vec![];

        for _ in 0..self.thread_count {
            let handle = thread::spawn({
                let provider = provider.clone();
                let node_executor = node_executor.clone();

                move || loop {
                    let node;
                    {
                        let mut provider_lock = provider.lock().unwrap();
                        if provider_lock.is_empty() {
                            break;
                        }

                        node = provider_lock.pop();
                    }

                    if let Some(node) = node {
                        node_executor.call(node.clone());

                        {
                            let mut provider_lock = provider.lock().unwrap();
                            provider_lock.complete(node);
                        }
                    } else {
                        thread::sleep(Duration::from_millis(100));
                    }
                }
            });
            handles.push(handle);
        }

        for handle in handles {
            handle.join().unwrap();
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::{HashMap, HashSet};

    use super::*;

    struct NodeExample {
        id: String,
        dependencies: Vec<String>,
    }

    impl Node<String> for NodeExample {
        fn id<'a>(&'a self) -> &'a String {
            &self.id
        }

        fn dependencies(&self) -> &Vec<String> {
            &self.dependencies
        }
    }

    struct ExecutorExample {
        dependency_graph: HashMap<usize, Vec<usize>>,
        seen: Arc<Mutex<HashSet<usize>>>,
    }

    impl ExecutorExample {
        fn new(dependency_graph: HashMap<usize, Vec<usize>>) -> Self {
            Self {
                dependency_graph,
                seen: Arc::new(Mutex::new(HashSet::new())),
            }
        }
    }

    impl CallableByID<usize> for ExecutorExample {
        fn call(&self, id: usize) {
            thread::sleep(Duration::from_micros(100));

            let mut seen = self.seen.lock().unwrap();
            seen.insert(id);

            for dep in &self.dependency_graph[&id] {
                assert!(seen.contains(&dep));
            }
        }
    }

    #[test]
    fn it_works_with_single_thread() {
        let mut nodes: HashMap<usize, Vec<usize>> = HashMap::new();

        nodes.insert(1, vec![]);
        nodes.insert(2, vec![1]);
        nodes.insert(3, vec![1]);
        nodes.insert(4, vec![]);
        nodes.insert(5, vec![]);
        nodes.insert(6, vec![2, 3]);
        nodes.insert(7, vec![3, 4]);
        nodes.insert(8, vec![6]);

        let topological_batch_provider = TopologicalBatchProvider::new(nodes.clone());
        let runner = ThreadPoolRunner::new(1);
        let executor = Arc::new(ExecutorExample::new(nodes));

        runner.run(topological_batch_provider.unwrap(), executor);
    }

    #[test]
    fn it_works_with_multiple_threads() {
        let mut nodes: HashMap<usize, Vec<usize>> = HashMap::new();

        nodes.insert(1, vec![]);
        nodes.insert(2, vec![1]);
        nodes.insert(3, vec![1]);
        nodes.insert(4, vec![]);
        nodes.insert(5, vec![]);
        nodes.insert(6, vec![2, 3]);
        nodes.insert(7, vec![3, 4]);
        nodes.insert(8, vec![6]);

        let topological_batch_provider = TopologicalBatchProvider::new(nodes.clone());
        let runner = ThreadPoolRunner::new(4);
        let executor = Arc::new(ExecutorExample::new(nodes));

        runner.run(topological_batch_provider.unwrap(), executor);
    }
}
