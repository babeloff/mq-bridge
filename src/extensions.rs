use crate::traits::{CustomEndpointFactory, CustomMiddlewareFactory};
use std::collections::HashMap;
use std::sync::{Arc, OnceLock, RwLock};

static CUSTOM_ENDPOINT_REGISTRY: OnceLock<RwLock<HashMap<String, Arc<dyn CustomEndpointFactory>>>> =
    OnceLock::new();
static CUSTOM_MIDDLEWARE_REGISTRY: OnceLock<
    RwLock<HashMap<String, Arc<dyn CustomMiddlewareFactory>>>,
> = OnceLock::new();

pub fn register_endpoint_factory(name: &str, factory: Arc<dyn CustomEndpointFactory>) {
    let registry = CUSTOM_ENDPOINT_REGISTRY.get_or_init(|| RwLock::new(HashMap::new()));
    let mut map = registry
        .write()
        .expect("Custom endpoint registry lock poisoned");
    map.insert(name.to_string(), factory);
}

pub fn get_endpoint_factory(name: &str) -> Option<Arc<dyn CustomEndpointFactory>> {
    let registry = CUSTOM_ENDPOINT_REGISTRY.get_or_init(|| RwLock::new(HashMap::new()));
    let map = registry
        .read()
        .expect("Custom endpoint registry lock poisoned");
    map.get(name).cloned()
}

pub fn register_middleware_factory(name: &str, factory: Arc<dyn CustomMiddlewareFactory>) {
    let registry = CUSTOM_MIDDLEWARE_REGISTRY.get_or_init(|| RwLock::new(HashMap::new()));
    let mut map = registry.write().expect("Middleware registry lock poisoned");
    map.insert(name.to_string(), factory);
}

pub fn get_middleware_factory(name: &str) -> Option<Arc<dyn CustomMiddlewareFactory>> {
    let registry = CUSTOM_MIDDLEWARE_REGISTRY.get_or_init(|| RwLock::new(HashMap::new()));
    let map = registry.read().expect("Middleware registry lock poisoned");
    map.get(name).cloned()
}
