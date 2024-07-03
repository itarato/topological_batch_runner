use super::common::*;
use std::{
    collections::{HashMap, HashSet},
    hash::Hash,
};

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

#[cfg(test)]
mod tests {
    use super::*;

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
}
