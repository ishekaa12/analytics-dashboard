/**
 * tracker.js — Lightweight analytics collector (<5KB)
 * Captures pageviews and click events, sends via navigator.sendBeacon.
 * Usage: <script src="/tracker.js"></script>
 */
(function () {
  "use strict";

  var ENDPOINT = "/collect";
  var sid =
    localStorage.getItem("_wa_sid") ||
    (function () {
      var s =
        Math.random().toString(36).slice(2) +
        Date.now().toString(36);
      localStorage.setItem("_wa_sid", s);
      return s;
    })();

  function vp() {
    return {
      w: window.innerWidth || document.documentElement.clientWidth,
      h: window.innerHeight || document.documentElement.clientHeight,
    };
  }

  function send(payload) {
    payload.session_id = sid;
    payload.user_agent = navigator.userAgent;
    payload.timestamp = new Date().toISOString();
    var v = vp();
    payload.vp_width = v.w;
    payload.vp_height = v.h;

    if (navigator.sendBeacon) {
      navigator.sendBeacon(
        ENDPOINT,
        new Blob([JSON.stringify(payload)], { type: "application/json" })
      );
    } else {
      var xhr = new XMLHttpRequest();
      xhr.open("POST", ENDPOINT, true);
      xhr.setRequestHeader("Content-Type", "application/json");
      xhr.send(JSON.stringify(payload));
    }
  }

  // ── Pageview ──
  function trackPageview() {
    send({
      event_type: "pageview",
      page_url: location.pathname + location.search,
      referrer: document.referrer || "",
    });
  }

  // ── Click ──
  function trackClick(e) {
    send({
      event_type: "click",
      page_url: location.pathname + location.search,
      referrer: document.referrer || "",
      click_x: Math.round(e.clientX),
      click_y: Math.round(e.clientY),
    });
  }

  // ── Init ──
  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", trackPageview);
  } else {
    trackPageview();
  }

  document.addEventListener("click", trackClick, { passive: true });
})();
