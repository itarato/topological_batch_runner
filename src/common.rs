use std::hash::Hash;

pub type Error = Box<dyn std::error::Error + Send + Sync>;

pub trait Node<T: Clone + Hash + PartialEq + Eq> {
    fn id(&self) -> &T;
    fn dependencies(&self) -> &Vec<T>;
}
pub trait CallableByID<T> {
    fn call(&self, id: T);
}
