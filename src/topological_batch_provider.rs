//! The topological batch provider can be used independently from the runner. It has added circular dependency
//! detection.

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
    /// The dependency list is expected as a map. All node must declare their dependecy, even when there is none.
    /// For example the following structure:
    ///
    /// ```text
    /// {
    ///     0 => [1],
    ///     1 => [],
    /// }
    /// ```
    ///
    /// Says: 0 depends on 1 (1 must come before 0) and 1 has no dependency.
    ///
    /// It returns an error when circular dependency is detected.
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

    /// Empty is a global check over the batch provider, when it has no more ID to provide and all of the retrieved
    /// IDs were marked as computed.
    pub fn is_empty(&self) -> bool {
        self.available.is_empty() && self.unavailable.is_empty()
    }

    /// Complete is the signal the resolution of the dependency - all of it's dependees are now free of this dependency.
    /// When all dependencies of a dependee are `complete`ed, the dependee is ready to be used.
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

    /// Get an available ID to be computed. It picks one random from the available batch.
    /// Getting a `None` only means that there is no more available in the current batch. Signaling `complete` on the
    /// actively computed IDs might yield new available items.
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

    #[test]
    fn it_provides_batches() {
        let mut nodes: HashMap<usize, Vec<usize>> = HashMap::new();

        nodes.insert(1, vec![]);
        nodes.insert(2, vec![1]);
        nodes.insert(3, vec![1]);
        nodes.insert(4, vec![]);
        nodes.insert(5, vec![]);
        nodes.insert(6, vec![2, 3]);
        nodes.insert(7, vec![3, 4]);
        nodes.insert(8, vec![6]);

        let mut topological_batch_provider = TopologicalBatchProvider::new(nodes.clone()).unwrap();

        let expected: Vec<Vec<usize>> = vec![vec![1, 4, 5], vec![2, 3], vec![6, 7], vec![8]];
        for i in 0..4 {
            let mut actual = HashSet::new();
            while let Some(v) = topological_batch_provider.pop() {
                actual.insert(v);
            }

            assert_eq!(
                HashSet::from_iter(expected.get(i).unwrap().into_iter().cloned()),
                actual
            );
            for v in actual {
                topological_batch_provider.complete(v);
            }
        }

        assert!(topological_batch_provider.is_empty());
    }
}
