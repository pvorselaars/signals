globalThis.resizeInterop = {
  getWidth: function (el) {
    if (!el) return 0;
    const r = el.getBoundingClientRect();
    return r?.width ? r.width : (el.clientWidth || 0);
  },

  observeResize: function (el, dotNetRef) {
    if (!el) return;
    if (typeof ResizeObserver === 'undefined') return;
    const ro = new ResizeObserver(function (entries) {
      for (const entry of entries) {
        const w = entry.contentRect ? entry.contentRect.width : entry.target.getBoundingClientRect().width;
        dotNetRef.invokeMethodAsync('NotifyWidthChanged', w);
      }
    });

    ro.observe(el);
    el.__ro = ro;
    el.__ref = dotNetRef;
  },

  unobserveResize: function (el) {
    if (el?.__ro) {
      try {
        el.__ro.disconnect();
        if (el.__ref?.dispose)
          el.__ref.dispose();
      } catch (e) { 
        console.log(e)
      }
      delete el.__ro;
      delete el.__fef;
    }
  }
};
