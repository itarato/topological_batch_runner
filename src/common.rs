pub type Error = Box<dyn std::error::Error + Send + Sync>;

pub trait CallableByID<T> {
    fn call(&self, id: T);
}
