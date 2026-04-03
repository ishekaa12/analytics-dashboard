(function() {
  'use strict';
  var endpoint = '/collect';
  var sessionId = Math.random().toString(36).substring(2);

  function send(data) {
    var payload = JSON.stringify(Object.assign({}, data, { sessionId: sessionId }));
    if (navigator.sendBeacon) {
      navigator.sendBeacon(endpoint, payload);
    } else {
      fetch(endpoint, { method: 'POST', body: payload, keepalive: true });
    }
  }

  function trackPageview() {
    send({
      page_url: location.href,
      referrer: document.referrer,
      timestamp: new Date().toISOString(),
      event_type: 'pageview',
      screen_width: window.innerWidth,
      screen_height: window.innerHeight,
      user_agent: navigator.userAgent
    });
  }

  function trackClick(e) {
    send({
      page_url: location.href,
      referrer: document.referrer,
      timestamp: new Date().toISOString(),
      event_type: 'click',
      click_x: e.clientX,
      click_y: e.clientY,
      screen_width: window.innerWidth,
      screen_height: window.innerHeight,
      user_agent: navigator.userAgent
    });
  }

  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', trackPageview);
  } else {
    trackPageview();
  }
  document.addEventListener('click', trackClick, true);
})();<script src="tracker.js"></script>