use crate::traits::{CustomEndpointFactory, CustomMiddlewareFactory};
use std::collections::HashMap;
use std::sync::{Arc, OnceLock, RwLock};

static CUSTOM_ENDPOINT_REGISTRY: OnceLock<RwLock<HashMap<String, Arc<dyn CustomEndpointFactory>>>> =
    OnceLock::new();
static CUSTOM_MIDDLEWARE_REGISTRY: OnceLock<
    RwLock<HashMap<String, Arc<dyn CustomMiddlewareFactory>>>,
> = OnceLock::new();

pub fn register_endpoint_factory(
    name: &str,
    factory: Arc<dyn CustomEndpointFactory>,
) -> anyhow::Result<()> {
    let registry = CUSTOM_ENDPOINT_REGISTRY.get_or_init(|| RwLock::new(HashMap::new()));
    let mut map = registry
        .write()
        .map_err(|_| anyhow::anyhow!("custom endpoint registry lock poisoned"))?;
    if map.contains_key(name) {
        return Err(anyhow::anyhow!(
            "an endpoint factory named `{name}` is already registered"
        ));
    }
    map.insert(name.to_string(), factory);
    Ok(())
}

pub fn get_endpoint_factory(name: &str) -> Option<Arc<dyn CustomEndpointFactory>> {
    let registry = CUSTOM_ENDPOINT_REGISTRY.get_or_init(|| RwLock::new(HashMap::new()));
    let map = registry.read().ok()?;
    map.get(name).cloned()
}

#[cfg(feature = "plugin")]
pub(crate) fn unregister_endpoint_factory(name: &str) {
    if let Some(registry) = CUSTOM_ENDPOINT_REGISTRY.get() {
        if let Ok(mut factories) = registry.write() {
            factories.remove(name);
        }
    }
}

pub fn register_middleware_factory(
    name: &str,
    factory: Arc<dyn CustomMiddlewareFactory>,
) -> anyhow::Result<()> {
    let registry = CUSTOM_MIDDLEWARE_REGISTRY.get_or_init(|| RwLock::new(HashMap::new()));
    let mut map = registry
        .write()
        .map_err(|_| anyhow::anyhow!("middleware registry lock poisoned"))?;
    if map.contains_key(name) {
        return Err(anyhow::anyhow!(
            "a middleware factory named `{name}` is already registered"
        ));
    }
    map.insert(name.to_string(), factory);
    Ok(())
}

pub fn get_middleware_factory(name: &str) -> Option<Arc<dyn CustomMiddlewareFactory>> {
    let registry = CUSTOM_MIDDLEWARE_REGISTRY.get_or_init(|| RwLock::new(HashMap::new()));
    let map = registry.read().ok()?;
    map.get(name).cloned()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Debug)]
    struct EndpointFactory;

    impl CustomEndpointFactory for EndpointFactory {}

    #[derive(Debug)]
    struct MiddlewareFactory;

    impl CustomMiddlewareFactory for MiddlewareFactory {}

    #[test]
    fn duplicate_endpoint_registration_is_rejected_without_replacing_the_factory() {
        let name = "extensions-test-duplicate-endpoint";
        let first: Arc<dyn CustomEndpointFactory> = Arc::new(EndpointFactory);

        register_endpoint_factory(name, Arc::clone(&first)).unwrap();
        let error = register_endpoint_factory(name, Arc::new(EndpointFactory)).unwrap_err();

        assert!(error.to_string().contains("already registered"));
        assert!(Arc::ptr_eq(&first, &get_endpoint_factory(name).unwrap()));
    }

    #[test]
    fn duplicate_middleware_registration_is_rejected_without_replacing_the_factory() {
        let name = "extensions-test-duplicate-middleware";
        let first: Arc<dyn CustomMiddlewareFactory> = Arc::new(MiddlewareFactory);

        register_middleware_factory(name, Arc::clone(&first)).unwrap();
        let error = register_middleware_factory(name, Arc::new(MiddlewareFactory)).unwrap_err();

        assert!(error.to_string().contains("already registered"));
        assert!(Arc::ptr_eq(&first, &get_middleware_factory(name).unwrap()));
    }
}
