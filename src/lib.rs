use std::{
    collections::{HashMap, HashSet},
    hash::Hash,
    sync::{Arc, Mutex},
    thread,
    time::Duration,
};

pub type Error = Box<dyn std::error::Error + Send + Sync>;

trait Node<T: Clone + Hash + PartialEq + Eq> {
    fn id(&self) -> &T;
    fn dependencies(&self) -> &Vec<T>;
}

#[derive(Debug)]
pub struct TopologicalBatchProvider<T> {
    unavailable: HashSet<T>,
    rights: Vec<T>,
    available: HashSet<T>,
    inverse_dependency: HashMap<T, Vec<T>>,
}

impl<T: Hash + PartialEq + Eq + Clone> TopologicalBatchProvider<T> {
    pub fn new(nodes: HashMap<T, Vec<T>>) -> Result<Self, Error> {
        if Self::has_cycle(&nodes) {
            return Err("Cycle detected.".into());
        }

        let mut inverse_dependency: HashMap<T, Vec<T>> = HashMap::new();
        let mut rights = vec![];
        let mut unavailable = HashSet::new();

        for (dependee, dependencies) in &nodes {
            unavailable.insert(dependee.clone());

            for dependency in dependencies {
                inverse_dependency
                    .entry(dependency.clone())
                    .or_default()
                    .push(dependee.clone());

                rights.push(dependee.clone());
            }
        }

        let available = unavailable
            .difference(&HashSet::from_iter(rights.iter().cloned()))
            .cloned()
            .collect::<HashSet<T>>();

        Ok(Self {
            unavailable,
            rights,
            available,
            inverse_dependency,
        })
    }

    fn has_cycle(nodes: &HashMap<T, Vec<T>>) -> bool {
        let mut done: HashMap<T, HashSet<T>> = HashMap::new();

        for (n, req) in nodes {
            let mut stack = req.clone();
            done.insert(n.clone(), HashSet::new());

            while let Some(m) = stack.pop() {
                if done[n].contains(&m) {
                    continue;
                }

                if &m == n {
                    return true;
                }

                for dep_m in &nodes[&m] {
                    stack.push(dep_m.clone());
                }

                done.get_mut(n).unwrap().insert(m);
            }
        }

        false
    }

    pub fn is_empty(&self) -> bool {
        self.available.is_empty() && self.unavailable.is_empty()
    }

    pub fn complete(&mut self, node: T) {
        if self.inverse_dependency.contains_key(&node) {
            for rev_dep_node in self.inverse_dependency.get_mut(&node).unwrap().drain(0..) {
                let i = self.rights.iter().position(|e| e == &rev_dep_node).unwrap();
                self.rights.remove(i);

                if !self.rights.contains(&rev_dep_node) {
                    self.available.insert(rev_dep_node);
                }
            }

            self.inverse_dependency.remove(&node);
        }

        self.unavailable.remove(&node);
    }

    pub fn pop(&mut self) -> Option<T> {
        if let Some(popped) = self.available.iter().next().cloned() {
            self.available.take(&popped)
        } else {
            None
        }
    }
}

pub trait CallableByID<T> {
    fn call(&self, id: T);
}

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
    fn it_detects_cycles() {
        let mut nodes: HashMap<usize, Vec<usize>> = HashMap::new();

        nodes.insert(1, vec![3, 4]);
        nodes.insert(2, vec![1]);
        nodes.insert(3, vec![2]);
        nodes.insert(4, vec![]);

        assert!(TopologicalBatchProvider::new(nodes).is_err());
    }

    #[test]
    fn it_detects_cycles_not_at_the_beginning() {
        let mut nodes: HashMap<usize, Vec<usize>> = HashMap::new();

        nodes.insert(1, vec![3]);
        nodes.insert(2, vec![3]);
        nodes.insert(3, vec![2]);

        assert!(TopologicalBatchProvider::new(nodes).is_err());
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
