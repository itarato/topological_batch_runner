use std::{
    collections::{HashMap, HashSet},
    sync::{Arc, Mutex},
    thread,
    time::Duration,
};

#[derive(Debug)]
pub struct TopologicalBatchProvider {
    unavailable: HashSet<usize>,
    rights: Vec<usize>,
    available: HashSet<usize>,
    inverse_dependency: HashMap<usize, Vec<usize>>,
}

impl TopologicalBatchProvider {
    pub fn new(dependency: HashMap<usize, Vec<usize>>) -> Self {
        let mut inverse_dependency: HashMap<usize, Vec<usize>> = HashMap::new();
        let mut rights = vec![];
        let mut unavailable = HashSet::new();

        for (dependee, dependencies) in &dependency {
            unavailable.insert(*dependee);

            for dependency in dependencies {
                inverse_dependency
                    .entry(*dependency)
                    .or_default()
                    .push(*dependee);

                rights.push(*dependee);
            }
        }

        let available = unavailable
            .difference(&HashSet::from_iter(rights.iter().copied()))
            .copied()
            .collect::<HashSet<usize>>();

        Self {
            unavailable,
            rights,
            available,
            inverse_dependency,
        }
    }

    pub fn is_empty(&self) -> bool {
        self.available.is_empty() && self.unavailable.is_empty()
    }

    pub fn complete(&mut self, node: usize) {
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

    pub fn pop(&mut self) -> Option<usize> {
        if let Some(popped) = self.available.iter().next().copied() {
            self.available.take(&popped)
        } else {
            None
        }
    }
}

pub struct ThreadPoolRunner {
    thread_count: usize,
}

impl ThreadPoolRunner {
    pub fn new(thread_count: usize) -> Self {
        Self { thread_count }
    }

    pub fn run(&self, topological_batch_provider: TopologicalBatchProvider) {
        let provider = Arc::new(Mutex::new(topological_batch_provider));
        let mut handles = vec![];

        for _ in 0..self.thread_count {
            let handle = thread::spawn({
                let provider = provider.clone();

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
                        println!("Start working on node {}", node);
                        thread::sleep(Duration::from_secs(1));
                        println!("Finish working on node {}", node);

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

    #[test]
    fn it_works() {
        let mut nodes: HashMap<usize, Vec<usize>> = HashMap::new();

        nodes.insert(1, vec![]);
        nodes.insert(2, vec![1]);
        nodes.insert(3, vec![1]);
        nodes.insert(4, vec![]);
        nodes.insert(5, vec![]);
        nodes.insert(6, vec![2, 3]);
        nodes.insert(7, vec![3, 4]);
        nodes.insert(8, vec![6]);

        let topological_batch_provider = TopologicalBatchProvider::new(nodes);
        dbg!(&topological_batch_provider);

        // while !topological_batch_provider.is_empty() {
        //     let mut batch = vec![];
        //     while let Some(node) = topological_batch_provider.pop() {
        //         batch.push(node);
        //     }

        //     dbg!(&batch);

        //     for node in batch {
        //         topological_batch_provider.complete(node);
        //     }
        // }

        let runner = ThreadPoolRunner::new(4);
        runner.run(topological_batch_provider);
    }
}
