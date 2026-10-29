// Ceresit Queue Daemon Service Worker for Web Push
self.addEventListener('push', function (event) {
    let data = { title: 'Ceresit Колірувальний Цех', body: 'Ваше замовлення готове до видачі!' };
    try {
        if (event.data) {
            data = event.data.json();
        }
    } catch (e) {
        if (event.data) {
            data.body = event.data.text();
        }
    }

    const options = {
        body: data.body,
        icon: '/static/icon-192.png',
        badge: '/static/badge-72.png',
        vibrate: [200, 100, 200, 100, 400],
        tag: 'cqd-order-ready',
        renotify: true,
        data: {
            url: data.url || '/'
        }
    };

    event.waitUntil(
        self.registration.showNotification(data.title || 'Ceresit Цех', options)
    );
});

self.addEventListener('notificationclick', function (event) {
    event.notification.close();
    const targetUrl = event.notification.data ? event.notification.data.url : '/';
    event.waitUntil(
        clients.matchAll({ type: 'window', includeUncontrolled: true }).then(function (clientList) {
            for (let i = 0; i < clientList.length; i++) {
                let client = clientList[i];
                if (client.url.includes(targetUrl) && 'focus' in client) {
                    return client.focus();
                }
            }
            if (clients.openWindow) {
                return clients.openWindow(targetUrl);
            }
        })
    );
});
