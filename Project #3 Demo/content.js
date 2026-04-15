// Inject page_script.js into the MAIN world via a <script> tag.
(function injectPageScript() {
  if (document.getElementById('type-into-doc-injected')) return;
  const script = document.createElement('script');
  script.id = 'type-into-doc-injected';
  script.src = chrome.runtime.getURL('page_script.js');
  (document.head || document.documentElement).appendChild(script);
})();

// Listen for messages from the side panel
chrome.runtime.onMessage.addListener((message, sender, sendResponse) => {

  if (message.type === 'PING') {
    sendResponse({ ok: true });
    return false;
  }

  if (message.type === 'TYPE_TEXT') {
    const handleResult = (event) => {
      window.removeEventListener('TYPE_INTO_DOC_RESULT', handleResult);
      sendResponse({ success: event.detail.success });
    };
    window.addEventListener('TYPE_INTO_DOC_RESULT', handleResult);
    window.postMessage({ type: 'TYPE_INTO_DOC', text: message.text, mode: message.mode || 'fast', deletions: message.deletions || [] }, '*');
    return true; // async
  }

  if (message.type === 'APPLY_DIFF') {
    const handleResult = (event) => {
      window.removeEventListener('APPLY_DIFF_RESULT', handleResult);
      sendResponse({ success: event.detail.success });
    };
    window.addEventListener('APPLY_DIFF_RESULT', handleResult);
    window.postMessage({ type: 'APPLY_DIFF_OPS', operations: message.operations, mode: message.mode || 'fast', resetCursor: !!message.resetCursor }, '*');
    return true; // async
  }

  if (message.type === 'CLEAR_DOC') {
    const handleResult = (event) => {
      window.removeEventListener('CLEAR_DOC_RESULT', handleResult);
      sendResponse({ success: event.detail.success });
    };
    window.addEventListener('CLEAR_DOC_RESULT', handleResult);
    window.postMessage({ type: 'CLEAR_DOC' }, '*');
    return true;
  }

  if (message.type === 'GET_TEXT') {
    const handleResult = (event) => {
      window.removeEventListener('GET_DOC_TEXT_RESULT', handleResult);
      sendResponse({ success: event.detail.success, text: event.detail.text || '' });
    };
    window.addEventListener('GET_DOC_TEXT_RESULT', handleResult);
    window.postMessage({ type: 'GET_DOC_TEXT' }, '*');
    return true; // async
  }

  if (message.type === 'PAUSE_TYPING') {
    window.postMessage({ type: 'TYPING_PAUSE' }, '*');
    return false;
  }

  if (message.type === 'RESUME_TYPING') {
    window.postMessage({ type: 'TYPING_RESUME' }, '*');
    return false;
  }

  if (message.type === 'STOP_TYPING') {
    window.postMessage({ type: 'TYPING_STOP' }, '*');
    return false;
  }
});
