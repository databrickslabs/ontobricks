(function () {
  "use strict";

  var reduceMotion = window.matchMedia("(prefers-reduced-motion: reduce)").matches;

  function initNav() {
    var toggle = document.querySelector("[data-nav-toggle]");
    var nav = document.querySelector("[data-site-nav]");
    if (!toggle || !nav) return;

    toggle.addEventListener("click", function () {
      var open = nav.classList.toggle("is-open");
      toggle.setAttribute("aria-expanded", open ? "true" : "false");
    });

    nav.querySelectorAll("a").forEach(function (link) {
      link.addEventListener("click", function () {
        nav.classList.remove("is-open");
        toggle.setAttribute("aria-expanded", "false");
      });
    });
  }

  function markActiveNav() {
    var path = window.location.pathname.replace(/\/$/, "") || "/";
    document.querySelectorAll("[data-site-nav] a").forEach(function (link) {
      var href = link.getAttribute("href");
      if (!href) return;
      var resolved = new URL(href, window.location.href).pathname.replace(/\/$/, "") || "/";
      if (resolved === path) {
        link.setAttribute("aria-current", "page");
      }
    });
  }

  function initHeroMotion() {
    var hero = document.querySelector(".hero");
    if (!hero) return;
    if (reduceMotion) return;
    requestAnimationFrame(function () {
      hero.classList.add("is-ready");
    });
  }

  initNav();
  markActiveNav();
  initHeroMotion();
})();
