// Runs in the MAIN world (same JS context as Google Docs).

// ── Typing control state ──────────────────────────────────
let isPaused = false;
let isStopped = false;

// Resolve function stored so RESUME_TYPING can unblock the paused loop
let resumeResolve = null;

window.addEventListener('message', (event) => {
  if (event.source !== window || !event.data) return;

  switch (event.data.type) {
    case 'TYPE_INTO_DOC':
      isStopped = false;
      isPaused  = false;
      typeTextIntoDoc(event.data.text, event.data.mode || 'fast', event.data.deletions || []);
      break;

    case 'APPLY_DIFF_OPS':
      isStopped = false;
      isPaused  = false;
      applyDiffOps(event.data.operations || [], event.data.mode || 'fast', !!event.data.resetCursor);
      break;

    case 'GET_DOC_TEXT':
      try {
        const paragraphs = document.querySelectorAll('.kix-paragraphrenderer');
        const lines = [];
        paragraphs.forEach(p => {
          lines.push(p.textContent.replace(/[\u200B\uFEFF]/g, ''));
        });
        // Trim trailing empty paragraphs (Google Docs always adds one)
        while (lines.length > 0 && lines[lines.length - 1] === '') lines.pop();
        window.dispatchEvent(new CustomEvent('GET_DOC_TEXT_RESULT', {
          detail: { success: true, text: lines.join('\n') }
        }));
      } catch (err) {
        window.dispatchEvent(new CustomEvent('GET_DOC_TEXT_RESULT', {
          detail: { success: false, text: '' }
        }));
      }
      break;

    case 'CLEAR_DOC':
      clearDocument();
      break;

    case 'TYPING_PAUSE':
      isPaused = true;
      break;

    case 'TYPING_RESUME':
      isPaused = false;
      if (resumeResolve) { resumeResolve(); resumeResolve = null; }
      break;

    case 'TYPING_STOP':
      isStopped = true;
      isPaused  = false;
      if (resumeResolve) { resumeResolve(); resumeResolve = null; }
      hideClickBlocker(); // Immediately remove overlay on stop
      break;
  }
});

// ── Click-blocking overlay ───────────────────────────────
// Prevents user clicks from moving Google Docs' cursor during operations
function showClickBlocker() {
  if (document.getElementById('mythos-click-blocker')) return;
  const overlay = document.createElement('div');
  overlay.id = 'mythos-click-blocker';
  overlay.style.cssText = 'position:fixed;top:0;left:0;width:100vw;height:100vh;z-index:2147483647;background:transparent;cursor:not-allowed;';

  const pill = document.createElement('div');
  pill.style.cssText = 'position:absolute;top:12px;left:50%;transform:translateX(-50%);background:rgba(0,0,0,0.78);color:#fff;padding:7px 18px;border-radius:20px;font-size:13px;font-family:-apple-system,BlinkMacSystemFont,sans-serif;display:flex;align-items:center;gap:8px;box-shadow:0 4px 16px rgba(0,0,0,0.18);pointer-events:none;';

  const dot = document.createElement('span');
  dot.style.cssText = 'width:7px;height:7px;border-radius:50%;background:#4ade80;display:inline-block;';
  // Pulse animation via inline style + keyframes injected once
  if (!document.getElementById('mythos-blocker-style')) {
    const style = document.createElement('style');
    style.id = 'mythos-blocker-style';
    style.textContent = '@keyframes mythos-pulse{0%,100%{opacity:1;transform:scale(1);}50%{opacity:0.4;transform:scale(0.8);}}';
    document.head.appendChild(style);
  }
  dot.style.animation = 'mythos-pulse 1.2s ease-in-out infinite';

  pill.appendChild(dot);
  pill.appendChild(document.createTextNode('Mythos is typing…'));
  overlay.appendChild(pill);
  document.body.appendChild(overlay);
}

function hideClickBlocker() {
  const el = document.getElementById('mythos-click-blocker');
  if (el) el.remove();
}

// ── Wait for pause to lift ────────────────────────────────
function waitIfPaused() {
  if (!isPaused) return Promise.resolve();
  return new Promise((resolve) => { resumeResolve = resolve; });
}

// ── Key info table ────────────────────────────────────────
function getKeyInfo(char) {
  if ((char >= 'a' && char <= 'z') || (char >= 'A' && char <= 'Z')) {
    const upper = char.toUpperCase();
    return { key: char, code: 'Key' + upper, keyCode: upper.charCodeAt(0), shiftKey: char >= 'A' && char <= 'Z' };
  }
  if (char >= '0' && char <= '9') {
    return { key: char, code: 'Digit' + char, keyCode: char.charCodeAt(0), shiftKey: false };
  }
  if (char === ' ') {
    return { key: ' ', code: 'Space', keyCode: 32, shiftKey: false };
  }
  const shiftedSymbols = {
    '!': { code: 'Digit1', keyCode: 49 },
    '@': { code: 'Digit2', keyCode: 50 },
    '#': { code: 'Digit3', keyCode: 51 },
    '$': { code: 'Digit4', keyCode: 52 },
    '%': { code: 'Digit5', keyCode: 53 },
    '^': { code: 'Digit6', keyCode: 54 },
    '&': { code: 'Digit7', keyCode: 55 },
    '*': { code: 'Digit8', keyCode: 56 },
    '(': { code: 'Digit9', keyCode: 57 },
    ')': { code: 'Digit0', keyCode: 48 },
  };
  if (shiftedSymbols[char]) return { key: char, ...shiftedSymbols[char], shiftKey: true };

  const punctuation = {
    '.': { code: 'Period',       keyCode: 190 },
    ',': { code: 'Comma',        keyCode: 188 },
    '-': { code: 'Minus',        keyCode: 189 },
    '=': { code: 'Equal',        keyCode: 187 },
    '[': { code: 'BracketLeft',  keyCode: 219 },
    ']': { code: 'BracketRight', keyCode: 221 },
    ';': { code: 'Semicolon',    keyCode: 186 },
    "'": { code: 'Quote',        keyCode: 222 },
    '`': { code: 'Backquote',    keyCode: 192 },
    '\\':{ code: 'Backslash',    keyCode: 220 },
    '/': { code: 'Slash',        keyCode: 191 },
  };
  if (punctuation[char]) return { key: char, ...punctuation[char], shiftKey: false };

  const shiftedPunct = {
    '_': { code: 'Minus',        keyCode: 189 },
    '+': { code: 'Equal',        keyCode: 187 },
    '{': { code: 'BracketLeft',  keyCode: 219 },
    '}': { code: 'BracketRight', keyCode: 221 },
    ':': { code: 'Semicolon',    keyCode: 186 },
    '"': { code: 'Quote',        keyCode: 222 },
    '~': { code: 'Backquote',    keyCode: 192 },
    '|': { code: 'Backslash',    keyCode: 220 },
    '<': { code: 'Comma',        keyCode: 188 },
    '>': { code: 'Period',       keyCode: 190 },
    '?': { code: 'Slash',        keyCode: 191 },
  };
  if (shiftedPunct[char]) return { key: char, ...shiftedPunct[char], shiftKey: true };

  return { key: char, code: 'Unidentified', keyCode: char.charCodeAt(0), shiftKey: false };
}

// ── Dispatch one character ────────────────────────────────
function dispatchCharacter(target, char) {
  if (char === '\n') {
    const e = { key: 'Enter', code: 'Enter', keyCode: 13, charCode: 13, which: 13,
                shiftKey: false, bubbles: true, cancelable: true, composed: true };
    target.dispatchEvent(new KeyboardEvent('keydown', e));
    target.dispatchEvent(new KeyboardEvent('keypress', e));
    target.dispatchEvent(new KeyboardEvent('keyup', e));
    return;
  }
  const info = getKeyInfo(char);
  const base = { key: info.key, code: info.code, keyCode: info.keyCode,
                 charCode: char.charCodeAt(0), which: info.keyCode, shiftKey: info.shiftKey,
                 bubbles: true, cancelable: true, composed: true };

  // Send full event sequence for every character.
  // getKeyInfo ensures keyCode is the PHYSICAL key code (e.g. 49 for '!', not 33),
  // so Google Docs won't misfire any keyboard shortcuts.
  target.dispatchEvent(new KeyboardEvent('keydown', base));
  target.dispatchEvent(new KeyboardEvent('keypress', base));
  target.dispatchEvent(new InputEvent('input', { data: char, inputType: 'insertText',
                                                 bubbles: true, cancelable: false, composed: true }));
  target.dispatchEvent(new KeyboardEvent('keyup', base));
}

// ── Timing modes ──────────────────────────────────────────
// All modes type at the SAME very fast speed.
// The only difference between fast/medium/human is pauses between states.
const FAST_CHAR_DELAY = () => 3 + Math.random() * 5;   // 3-8ms per char
const MODE_DELAYS = {
  fast:   FAST_CHAR_DELAY,
  medium: FAST_CHAR_DELAY,
  human:  FAST_CHAR_DELAY,
};

// ── Cursor operation timing (per arrow key / backspace) ───
// Cursor ops need a higher floor than typing — if Google Docs drops even
// ONE arrow-key event the cursor drifts and everything after is corrupted.
// 6-12ms gives ~80-160 arrows/sec which is very fast but reliable.
const FAST_CURSOR_DELAY = () => 6 + Math.random() * 6;  // 6-12ms
const CURSOR_DELAYS = {
  fast:   FAST_CURSOR_DELAY,
  medium: FAST_CURSOR_DELAY,
  human:  FAST_CURSOR_DELAY,
};
const BACKSPACE_DELAYS = { fast: 8, medium: 8, human: 8 };

// ── Core char-by-char loop (pause/stop aware) ─────────────
async function typeChars(target, text, delayFn) {
  for (let i = 0; i < text.length; i++) {
    if (isStopped) return false;
    await waitIfPaused();
    if (isStopped) return false;

    const char = text[i];
    dispatchCharacter(target, char);

    const delay = delayFn(char);
    if (delay > 0) await sleep(delay);
  }
  return true;
}

// ── Human mode: sentence-by-sentence with long pauses ─────
function splitIntoSentences(text) {
  return text.split(/(?<=[.!?])\s+/);
}

async function typeHuman(target, text) {
  const sentences = splitIntoSentences(text);
  let nextPauseAt = 3 + Math.floor(Math.random() * 2);

  for (let s = 0; s < sentences.length; s++) {
    if (isStopped) return false;

    // Typing speed is the same fast speed — only pauses differ
    const ok = await typeChars(target, sentences[s], FAST_CHAR_DELAY);
    if (!ok) return false;

    const isLast = s === sentences.length - 1;
    if (!isLast) {
      dispatchCharacter(target, ' ');

      if (s + 1 >= nextPauseAt) {
        nextPauseAt = s + 1 + 3 + Math.floor(Math.random() * 2);
        // Long pause — but still respect pause/stop
        const longMs = (40 + Math.random() * 20) * 1000;
        await interruptibleSleep(longMs);
      } else {
        await interruptibleSleep(200 + Math.random() * 300);
      }
      if (isStopped) return false;
    }
  }
  return true;
}

// ── Dispatch a special key (arrow, backspace) ────────────
function dispatchSpecialKey(target, key, code, keyCode, shiftKey = false) {
  const opts = { key, code, keyCode, which: keyCode, shiftKey,
                 bubbles: true, cancelable: true, composed: true };
  target.dispatchEvent(new KeyboardEvent('keydown', opts));
  target.dispatchEvent(new KeyboardEvent('keyup', opts));
}

// ── Deletion phase: navigate back and delete marked segments ──
async function deletionPhase(target, textLength, deletions) {
  if (!deletions.length) return true;

  // Sort deletions right-to-left (process from end of text first)
  const sorted = [...deletions].sort((a, b) => b.start - a.start);

  // Cursor is at the end of the typed text
  let cursorPos = textLength;

  for (let d = 0; d < sorted.length; d++) {
    if (isStopped) return false;
    await waitIfPaused();
    if (isStopped) return false;

    const { start, end } = sorted[d];
    const segLen = end - start;

    // Move cursor left from current position to the END of this segment
    const moveLeft = cursorPos - end;
    for (let i = 0; i < moveLeft; i++) {
      if (isStopped) return false;
      await waitIfPaused();
      dispatchSpecialKey(target, 'ArrowLeft', 'ArrowLeft', 37);
      await sleep(FAST_CURSOR_DELAY());
    }
    cursorPos = end;

    // Shift+Left to select the segment (character by character)
    for (let i = 0; i < segLen; i++) {
      if (isStopped) return false;
      await waitIfPaused();
      dispatchSpecialKey(target, 'ArrowLeft', 'ArrowLeft', 37, true);
      await sleep(FAST_CURSOR_DELAY());
    }

    // Backspace to delete the selection
    if (isStopped) return false;
    await waitIfPaused();
    dispatchSpecialKey(target, 'Backspace', 'Backspace', 8);
    await sleep(8);

    // Cursor is now at `start` and the text after is shorter
    cursorPos = start;

    // Wait 45 seconds before next deletion (interruptible)
    if (d < sorted.length - 1) {
      await interruptibleSleep(45000);
      if (isStopped) return false;
    }
  }

  return true;
}

// ── Clear document (Ctrl/Cmd+A then Backspace) ───────────
async function clearDocument() {
  try {
    const iframe = await waitForIframe();
    const iframeBody = iframe.contentDocument.body;
    iframe.focus();
    iframeBody.focus();

    const isMac = /Mac|iPhone|iPad/.test(navigator.platform);
    const selectOpts = {
      key: 'a', code: 'KeyA', keyCode: 65, which: 65,
      metaKey: isMac, ctrlKey: !isMac, shiftKey: false,
      bubbles: true, cancelable: true, composed: true,
    };
    iframeBody.dispatchEvent(new KeyboardEvent('keydown', selectOpts));
    iframeBody.dispatchEvent(new KeyboardEvent('keyup', selectOpts));
    await sleep(200);

    dispatchSpecialKey(iframeBody, 'Backspace', 'Backspace', 8);
    await sleep(200);

    window.dispatchEvent(new CustomEvent('CLEAR_DOC_RESULT', {
      detail: { success: true },
    }));
  } catch {
    window.dispatchEvent(new CustomEvent('CLEAR_DOC_RESULT', {
      detail: { success: false },
    }));
  }
}

// ── Entry point ───────────────────────────────────────────
function typeTextIntoDoc(text, mode, deletions) {
  waitForIframe()
    .then(async (iframe) => {
      const iframeBody = iframe.contentDocument.body;
      iframe.focus();
      iframeBody.focus();
      showClickBlocker();

      try {
        // Phase 1: Type everything
        let success;
        if (mode === 'human') {
          success = await typeHuman(iframeBody, text);
        } else {
          const delayFn = MODE_DELAYS[mode] || MODE_DELAYS.fast;
          success = await typeChars(iframeBody, text, delayFn);
        }

        // Phase 2: Delete marked segments
        if (success && !isStopped && deletions && deletions.length > 0) {
          success = await deletionPhase(iframeBody, text.length, deletions);
        }

        notifyResult(success && !isStopped);
      } finally {
        hideClickBlocker();
      }
    })
    .catch(() => { hideClickBlocker(); notifyResult(false); });
}

// ── Helpers ───────────────────────────────────────────────
function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

// Sleep that can be cut short by stop
function interruptibleSleep(ms) {
  return new Promise((resolve) => {
    const start = Date.now();
    function tick() {
      if (isStopped || !isPaused && Date.now() - start >= ms) { resolve(); return; }
      setTimeout(tick, 50);
    }
    setTimeout(tick, 50);
  });
}

function waitForIframe(timeout = 5000) {
  return new Promise((resolve, reject) => {
    const existing = document.querySelector('iframe.docs-texteventtarget-iframe');
    if (existing && existing.contentDocument) { resolve(existing); return; }
    const observer = new MutationObserver(() => {
      const iframe = document.querySelector('iframe.docs-texteventtarget-iframe');
      if (iframe && iframe.contentDocument) { observer.disconnect(); resolve(iframe); }
    });
    observer.observe(document.body, { childList: true, subtree: true });
    setTimeout(() => { observer.disconnect(); reject(new Error('iframe not found')); }, timeout);
  });
}

function notifyResult(success) {
  window.dispatchEvent(new CustomEvent('TYPE_INTO_DOC_RESULT', { detail: { success } }));
}

// ── Diff-based operations engine ─────────────────────────
function notifyDiffResult(success) {
  window.dispatchEvent(new CustomEvent('APPLY_DIFF_RESULT', { detail: { success } }));
}

function dispatchCtrlHome(target) {
  const isMac = /Mac|iPhone|iPad/.test(navigator.platform);
  // Google Docs uses Cmd+Up on Mac (Fn+Left/Home is unreliable)
  const opts = isMac
    ? { key: 'ArrowUp', code: 'ArrowUp', keyCode: 38, which: 38,
        metaKey: true, ctrlKey: false, shiftKey: false,
        bubbles: true, cancelable: true, composed: true }
    : { key: 'Home', code: 'Home', keyCode: 36, which: 36,
        metaKey: false, ctrlKey: true, shiftKey: false,
        bubbles: true, cancelable: true, composed: true };
  target.dispatchEvent(new KeyboardEvent('keydown', opts));
  target.dispatchEvent(new KeyboardEvent('keyup', opts));
}

async function applyDiffOps(operations, mode, resetCursor = true) {
  let success = false;
  try {
    const iframe = await waitForIframe();
    const iframeBody = iframe.contentDocument.body;
    iframe.focus();
    iframeBody.focus();
    showClickBlocker();

    // Move cursor to document start — required before every diff application
    // because cursor ops always assume starting at position 0
    if (resetCursor) {
      dispatchCtrlHome(iframeBody);
      await sleep(120); // Give Docs time to fully process the jump
    }

    const delayFn = MODE_DELAYS[mode] || MODE_DELAYS.fast;
    const cursorDelayFn = CURSOR_DELAYS[mode] || CURSOR_DELAYS.fast;
    const bsDelay = BACKSPACE_DELAYS[mode] || BACKSPACE_DELAYS.fast;
    let aborted = false;

    for (const op of operations) {
      if (isStopped) { aborted = true; break; }
      await waitIfPaused();
      if (isStopped) { aborted = true; break; }

      switch (op.action) {
        case 'navigate':
          for (let i = 0; i < op.count; i++) {
            if (isStopped) { aborted = true; break; }
            await waitIfPaused();
            dispatchSpecialKey(iframeBody, 'ArrowRight', 'ArrowRight', 39);
            await sleep(cursorDelayFn());
            // Every 80 arrow keys, pause briefly to let Google Docs catch up
            if (i > 0 && i % 80 === 0) await sleep(30);
          }
          break;

        case 'delete':
          for (let i = 0; i < op.count; i++) {
            if (isStopped) { aborted = true; break; }
            await waitIfPaused();
            dispatchSpecialKey(iframeBody, 'ArrowRight', 'ArrowRight', 39, true);
            await sleep(cursorDelayFn());
            if (i > 0 && i % 80 === 0) await sleep(30);
          }
          if (!aborted) {
            dispatchSpecialKey(iframeBody, 'Backspace', 'Backspace', 8);
            await sleep(bsDelay);
          }
          break;

        case 'insert':
          const ok = await typeChars(iframeBody, op.text, delayFn);
          if (!ok) aborted = true;
          break;
      }
      if (aborted) break;
    }

    success = !aborted;
  } catch {
    success = false;
  } finally {
    hideClickBlocker();
    notifyDiffResult(success);
  }
}
