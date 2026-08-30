(function () {
    const invoke = window.__TAURI__?.core?.invoke;
    if (typeof invoke !== 'function') {
        return;
    };

    const toResponse = (bridgeResponse) => {
        const headers = new Headers(bridgeResponse.headers || {});
        if (bridgeResponse.content_type) {
            headers.set('Content-Type', bridgeResponse.content_type);
        }

        const body = bridgeResponse.body_json != null
            ? JSON.stringify(bridgeResponse.body_json)
            : (bridgeResponse.body_text || '');

        return new Response(body, {
            status: bridgeResponse.status || 200,
            headers,
        });
    };

    const parseBody = async (requestLike) => {
        if (!requestLike) return null;
        const raw = requestLike.body;
        if (raw == null) return null;
        if (typeof raw === 'string') return raw;
        if (raw instanceof URLSearchParams) return raw.toString();
        if (raw instanceof Blob) return await raw.text();
        return String(raw);
    };

    window.__MQB_DESKTOP__ = true;

    const originalFetch = window.fetch.bind(window);

    // This function determines if a fetch request should be intercepted by Tauri
    const isLocalUiRequest = (url) => {
        if (!url || url.origin !== window.location.origin) return false;
        const pathname = url.pathname.split('#')[0]; // Strip hash fragment for routing
        return ['/health', '/schema.json', '/config', '/desktop-secrets', '/consumer-status', '/consumer-start', '/consumer-stop', '/messages', '/publish', '/runtime-status', '/peer-status', '/metrics'].includes(pathname);
    };

    // Override fetch for local UI requests
    window.fetch = async function tauriAwareFetch(input, init = {}) {
        const request = input instanceof Request ? input : null;
        const url = new URL(request ? request.url : String(input), window.location.origin);
        const normalizedInit = {
            method: init.method !== undefined ? init.method : request?.method || 'GET',
            body: init.body ?? (request ? await request.clone().text().catch(() => null) : null),
        };

        if (!isLocalUiRequest(url)) {
            return originalFetch(input, init);
        }

        const method = normalizedInit.method.toUpperCase();
        const pathname = url.pathname.split('#')[0]; // Strip hash fragment for routing
        const queryParams = Object.fromEntries(url.searchParams.entries()); // Get query params as object
        const bodyText = await parseBody(normalizedInit) || '';
        const consumer = queryParams.consumer || queryParams.consumer_id;

        let commandName;
        let commandArgs = {};

        // Map paths and methods to specific Tauri commands
        switch (pathname) {
            case '/health':
                commandName = 'get_health_request';
                break;
            case '/schema.json':
                commandName = 'get_schema_request';
                break;
            case '/config':
                if (method === 'GET') {
                    commandName = 'get_config_request';
                } else if (method === 'POST') {
                    commandName = 'post_config_request';
                    commandArgs = { bodyText };
                }
                break;
            case '/desktop-secrets':
                if (method === 'GET') {
                    commandName = 'get_desktop_secrets_request';
                } else if (method === 'DELETE') {
                    commandName = 'delete_desktop_secrets_request';
                }
                break;
            case '/consumer-status':
                commandName = 'get_consumer_status_request';
                commandArgs = { consumer: consumer || '' };
                break;
            case '/consumer-start':
                commandName = 'post_consumer_start_request';
                commandArgs = { consumer: consumer || '' };
                break;
            case '/consumer-stop':
                commandName = 'post_consumer_stop_request';
                commandArgs = { consumer: consumer || '' };
                break;
            case '/messages':
                commandName = 'get_messages_request';
                commandArgs = { consumer: consumer || null };
                break;
            case '/publish':
                commandName = 'post_publish_request';
                commandArgs = { bodyText };
                break;
            case '/runtime-status':
                commandName = 'get_runtime_status_request';
                break;
            case '/peer-status':
                commandName = 'get_peer_status_request';
                break;
            case '/metrics':
                commandName = 'get_metrics_request';
                break;
            default:
                // Fallback to original fetch if no specific command is found for a local UI request
                console.warn(`Unhandled local UI request in Tauri bridge: ${method} ${pathname}`);
                return originalFetch(input, init);
        }

        try {
            const bridgeResponse = await invoke(commandName, commandArgs);
            return toResponse(bridgeResponse);
        } catch (error) {
            // Wrap string errors from Rust into Error objects to prevent "undefined" messages in UI
            const message = typeof error === 'string' ? error : (error?.message || 'Tauri bridge invocation failed');
            throw new Error(message);
        }
    };
})();
