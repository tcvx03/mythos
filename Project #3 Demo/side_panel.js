// ============================================================
// Utility
// ============================================================
function escapeHTML(str) {
  return str.replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;').replace(/"/g, '&quot;').replace(/\n/g, '<br>');
}

// ============================================================
// Streaming: constants, abort controller, SSE parser
// ============================================================
// Claude: thinking tokens are SEPARATE from max_tokens (don't count against it)
// Gemini: thinking tokens SHARE the maxOutputTokens budget
const CLAUDE_THINKING_BUDGETS = { low: 4096, medium: 10000, high: 24576 };
const GEMINI_THINKING_BUDGETS = { low: 4096, medium: 16384, high: 32768 };
let streamAbortController = null;

async function* parseSSEStream(reader) {
  const decoder = new TextDecoder();
  let buffer = '';
  while (true) {
    const { done, value } = await reader.read();
    if (done) break;
    buffer += decoder.decode(value, { stream: true });
    const lines = buffer.split('\n');
    buffer = lines.pop();
    for (const line of lines) {
      if (line.startsWith('data: ')) {
        const jsonStr = line.slice(6);
        if (jsonStr === '[DONE]') return;
        try { yield JSON.parse(jsonStr); } catch { /* skip malformed */ }
      }
    }
  }
}

// ============================================================
// Streaming: Claude
// ============================================================
async function streamClaude({ apiKey, model, prompt, system, reasoning, maxTokens, onThinking, onText, signal }) {
  const selectedModel = model || 'claude-sonnet-4-6';
  const budget = CLAUDE_THINKING_BUDGETS[reasoning];
  // Claude: thinking is separate — max_tokens only limits visible output.
  // Sonnet max: 64k, Opus max: 128k. Use provided maxTokens or default high.
  // budget_tokens must be strictly less than max_tokens.
  const effectiveMaxTokens = maxTokens || 16000;
  const safeMaxTokens = budget ? Math.max(effectiveMaxTokens, budget + 1024) : effectiveMaxTokens;

  const body = {
    model: selectedModel,
    max_tokens: safeMaxTokens,
    messages: [{ role: 'user', content: prompt }],
    stream: true,
  };
  if (system) {
    body.system = [{ type: 'text', text: system }];
  }
  if (budget) {
    body.thinking = { type: 'enabled', budget_tokens: budget };
  }

  const res = await fetch('https://api.anthropic.com/v1/messages', {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
      'x-api-key': apiKey,
      'anthropic-version': '2023-06-01',
      'anthropic-dangerous-direct-browser-access': 'true',
    },
    body: JSON.stringify(body),
    signal,
  });

  if (!res.ok) {
    const err = await res.json().catch(() => ({}));
    throw new Error(err?.error?.message || `Claude API error ${res.status}`);
  }

  const reader = res.body.getReader();
  for await (const event of parseSSEStream(reader)) {
    if (signal?.aborted) break;
    if (event.type === 'content_block_delta') {
      if (event.delta?.type === 'thinking_delta') {
        onThinking(event.delta.thinking);
      } else if (event.delta?.type === 'text_delta') {
        onText(event.delta.text);
      }
    }
  }
}

// ============================================================
// Streaming: Gemini
// ============================================================
async function streamGemini({ apiKey, model, prompt, system, reasoning, maxTokens, onThinking, onText, signal }) {
  const selectedModel = model || 'gemini-3-flash-preview';
  const url = `https://generativelanguage.googleapis.com/v1beta/models/${selectedModel}:streamGenerateContent?alt=sse&key=${apiKey}`;
  const budget = GEMINI_THINKING_BUDGETS[reasoning];
  // Gemini: thinking tokens SHARE the maxOutputTokens budget (max 65536).
  // So we set it to the max and let thinking + output share the pool.
  const effectiveMaxTokens = maxTokens || 16000;
  const safeMaxTokens = budget ? Math.min(effectiveMaxTokens + budget, 65536) : effectiveMaxTokens;

  const body = {
    contents: [{ role: 'user', parts: [{ text: prompt }] }],
    generationConfig: { maxOutputTokens: Math.min(safeMaxTokens, 65536), temperature: 0.7 },
  };
  if (system) {
    body.systemInstruction = { parts: [{ text: system }] };
  }
  if (budget) {
    body.generationConfig.thinkingConfig = { thinkingBudget: budget, includeThoughts: true };
  }

  const res = await fetch(url, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(body),
    signal,
  });

  if (!res.ok) {
    const err = await res.json().catch(() => ({}));
    throw new Error(err?.error?.message || `Gemini API error ${res.status}`);
  }

  const reader = res.body.getReader();
  for await (const event of parseSSEStream(reader)) {
    if (signal?.aborted) break;
    const parts = event.candidates?.[0]?.content?.parts;
    if (!parts) continue;
    for (const part of parts) {
      if (part.thought === true) {
        onThinking(part.text || '');
      } else {
        onText(part.text || '');
      }
    }
  }
}

// ============================================================
// Text diff algorithm (LCS-based with prefix/suffix optimization)
// ============================================================
function computeDiff(oldText, newText) {
  if (oldText === newText) return [{ type: 'equal', text: oldText }];
  if (oldText.length === 0) return [{ type: 'insert', text: newText }];
  if (newText.length === 0) return [{ type: 'delete', text: oldText }];

  // Find common prefix
  let prefixLen = 0;
  const minLen = Math.min(oldText.length, newText.length);
  while (prefixLen < minLen && oldText[prefixLen] === newText[prefixLen]) prefixLen++;

  // Find common suffix (not overlapping with prefix)
  let suffixLen = 0;
  while (
    suffixLen < oldText.length - prefixLen &&
    suffixLen < newText.length - prefixLen &&
    oldText[oldText.length - 1 - suffixLen] === newText[newText.length - 1 - suffixLen]
  ) suffixLen++;

  const oldMiddle = oldText.slice(prefixLen, oldText.length - suffixLen);
  const newMiddle = newText.slice(prefixLen, newText.length - suffixLen);

  // Build result
  const ops = [];
  if (prefixLen > 0) ops.push({ type: 'equal', text: oldText.slice(0, prefixLen) });

  if (oldMiddle.length === 0 && newMiddle.length > 0) {
    ops.push({ type: 'insert', text: newMiddle });
  } else if (newMiddle.length === 0 && oldMiddle.length > 0) {
    ops.push({ type: 'delete', text: oldMiddle });
  } else if (oldMiddle.length > 0 && newMiddle.length > 0) {
    // LCS diff on the middle portion
    const lcsDiff = lcsMiddleDiff(oldMiddle, newMiddle);
    ops.push(...lcsDiff);
  }

  if (suffixLen > 0) ops.push({ type: 'equal', text: oldText.slice(oldText.length - suffixLen) });
  return ops;
}

function lcsMiddleDiff(oldStr, newStr) {
  const m = oldStr.length;
  const n = newStr.length;

  // Build LCS table (using two rows for memory efficiency)
  let prev = new Uint16Array(n + 1);
  let curr = new Uint16Array(n + 1);

  // We also need to backtrack, so store the full table for small inputs
  // For larger inputs, use the two-row approach with a direction table
  const dir = []; // 0 = diagonal (match), 1 = up (delete), 2 = left (insert)
  for (let i = 0; i <= m; i++) {
    dir.push(new Uint8Array(n + 1));
  }

  for (let i = 1; i <= m; i++) {
    [prev, curr] = [curr, prev];
    curr[0] = 0;
    for (let j = 1; j <= n; j++) {
      if (oldStr[i - 1] === newStr[j - 1]) {
        curr[j] = prev[j - 1] + 1;
        dir[i][j] = 0; // diagonal
      } else if (prev[j] >= curr[j - 1]) {
        curr[j] = prev[j];
        dir[i][j] = 1; // up
      } else {
        curr[j] = curr[j - 1];
        dir[i][j] = 2; // left
      }
    }
  }

  // Backtrack to build operations
  const rawOps = [];
  let i = m, j = n;
  while (i > 0 || j > 0) {
    if (i > 0 && j > 0 && dir[i][j] === 0) {
      rawOps.push({ type: 'equal', char: oldStr[i - 1] });
      i--; j--;
    } else if (i > 0 && (j === 0 || dir[i][j] === 1)) {
      rawOps.push({ type: 'delete', char: oldStr[i - 1] });
      i--;
    } else {
      rawOps.push({ type: 'insert', char: newStr[j - 1] });
      j--;
    }
  }
  rawOps.reverse();

  // Merge consecutive ops of same type
  const merged = [];
  for (const op of rawOps) {
    if (merged.length > 0 && merged[merged.length - 1].type === op.type) {
      merged[merged.length - 1].text += op.char;
    } else {
      merged.push({ type: op.type, text: op.char });
    }
  }
  return merged;
}

// Convert diff operations to cursor commands for the engine
function diffToCursorOps(diffOps) {
  const ops = [];
  for (const d of diffOps) {
    switch (d.type) {
      case 'equal':
        ops.push({ action: 'navigate', count: d.text.length });
        break;
      case 'delete':
        ops.push({ action: 'delete', count: d.text.length });
        break;
      case 'insert':
        ops.push({ action: 'insert', text: d.text });
        break;
    }
  }
  // Merge adjacent navigates (shouldn't normally happen, but safety)
  const merged = [];
  for (const op of ops) {
    if (op.action === 'navigate' && merged.length > 0 && merged[merged.length - 1].action === 'navigate') {
      merged[merged.length - 1].count += op.count;
    } else {
      merged.push(op);
    }
  }
  return merged;
}

// ============================================================
// Cinematic thinking stream UI helpers
// ============================================================
function showThinkingSection() {
  const section = document.getElementById('ai-thinking-section');
  const label = document.getElementById('ai-thinking-label');
  const content = document.getElementById('ai-thinking-content');

  content.textContent = '';
  gsap.set(content, { y: 0 });
  label.textContent = 'Thinking';
  label.classList.add('thinking-label-streaming');
  section.style.display = 'block';
  gsap.fromTo(section,
    { opacity: 0, height: 0 },
    { opacity: 1, height: 'auto', duration: 0.35, ease: 'power2.out' }
  );
}

function appendThinkingText(chunk) {
  const content = document.getElementById('ai-thinking-content');
  const window_ = document.getElementById('ai-thinking-body');
  content.textContent += chunk;

  // Auto-scroll: move content up so the latest text is always in the visible window
  const overflow = content.scrollHeight - window_.clientHeight;
  if (overflow > 0) {
    gsap.to(content, { y: -overflow, duration: 0.3, ease: 'power1.out', overwrite: true });
  }
}

function finishThinkingSection() {
  const label = document.getElementById('ai-thinking-label');
  label.textContent = 'Thinking — done';
  label.classList.remove('thinking-label-streaming');
}

// ============================================================
// Tab indicator setup
// ============================================================
const tabIndicator = document.querySelector('.tab-indicator');
const tabButtons = document.querySelectorAll('.tab-btn');

function positionIndicator(btn, animate = true) {
  // Use offsetLeft/offsetWidth — these are relative to the offset parent (.tabs)
  // and work reliably even before full paint, unlike getBoundingClientRect.
  const left = btn.offsetLeft;
  const width = btn.offsetWidth;

  if (animate) {
    gsap.to(tabIndicator, { left, width, duration: 0.3, ease: 'power2.inOut' });
  } else {
    gsap.set(tabIndicator, { left, width });
  }
}

// Initialize after layout is stable
requestAnimationFrame(() => {
  const activeTab = document.querySelector('.tab-btn.active');
  if (activeTab) positionIndicator(activeTab, false);
});

// Reposition on resize so the pill tracks correctly when the side panel is dragged
window.addEventListener('resize', () => {
  const activeTab = document.querySelector('.tab-btn.active');
  if (activeTab) positionIndicator(activeTab, false);
});

// ============================================================
// Tab switching with GSAP
// ============================================================
let isTabSwitching = false;

tabButtons.forEach((btn) => {
  btn.addEventListener('click', () => {
    if (isTabSwitching || btn.classList.contains('active')) return;

    isTabSwitching = true;
    const oldActive = document.querySelector('.tab-btn.active');
    const oldPanel = document.querySelector('.panel.active');
    const newPanel = document.getElementById('tab-' + btn.dataset.tab);

    // Update tab button states
    tabButtons.forEach(b => b.classList.remove('active'));
    btn.classList.add('active');

    // Slide indicator
    positionIndicator(btn, true);

    // Animate old panel out
    gsap.to(oldPanel, {
      opacity: 0,
      y: 12,
      duration: 0.2,
      ease: 'power2.in',
      onComplete: () => {
        oldPanel.classList.remove('active');
        gsap.set(oldPanel, { opacity: 1, y: 0 });

        // Show new panel
        newPanel.classList.add('active');
        gsap.set(newPanel, { opacity: 0, y: -12 });
        gsap.to(newPanel, { opacity: 1, y: 0, duration: 0.25, ease: 'power2.out' });

        // Position mode indicators now that panel is visible
        newPanel.querySelectorAll('.mode-options').forEach((c) => positionModeIndicator(c, false));

        // Stagger cards in
        const cards = newPanel.querySelectorAll('.card');
        if (cards.length) {
          gsap.fromTo(cards,
            { opacity: 0, y: 16 },
            { opacity: 1, y: 0, duration: 0.35, stagger: 0.08, ease: 'power3.out', delay: 0.05 }
          );
        }

        isTabSwitching = false;
      },
    });
  });
});

// ============================================================
// Human mode note toggle with GSAP
// ============================================================
function bindHumanNote(radioName, noteId) {
  const note = document.getElementById(noteId);
  document.querySelectorAll(`input[name="${radioName}"]`).forEach((r) => {
    r.addEventListener('change', () => {
      if (r.value === 'human') {
        note.style.display = 'block';
        gsap.fromTo(note,
          { opacity: 0, height: 0, paddingTop: 0, paddingBottom: 0 },
          { opacity: 1, height: 'auto', paddingTop: 7, paddingBottom: 7, duration: 0.35, ease: 'power2.out' }
        );
      } else {
        gsap.to(note, {
          opacity: 0, height: 0, paddingTop: 0, paddingBottom: 0,
          duration: 0.25, ease: 'power2.in',
          onComplete: () => { note.style.display = 'none'; },
        });
      }
    });
  });
}
bindHumanNote('paste-mode', 'paste-human-note');
// (ai-mode human note removed — AI tab no longer uses inline speed selector)

// ============================================================
// Load saved settings on open
// ============================================================
chrome.storage.local.get(['claudeKey', 'claudeModel', 'geminiKey', 'geminiModel'], (data) => {
  if (data.claudeKey)   { document.getElementById('claude-key').value   = data.claudeKey; }
  if (data.claudeModel) { document.getElementById('claude-model').value  = data.claudeModel; }
  if (data.geminiKey)   { document.getElementById('gemini-key').value   = data.geminiKey; }
  if (data.geminiModel) { document.getElementById('gemini-model').value  = data.geminiModel; }
});

// ============================================================
// Restore saved project on open
// ============================================================
(async () => {
  const project = await loadProject();
  if (project) {
    // Restore input fields
    if (project.topic) document.getElementById('ai-topic').value = project.topic;
    if (project.context) document.getElementById('ai-context').value = project.context;
    if (project.provider) document.getElementById('ai-provider').value = project.provider;

    // Restore days and reasoning radio selections
    if (project.numDays) {
      const daysVal = project.numDays <= 7 ? String(project.numDays) : 'auto';
      const daysRadio = document.querySelector(`input[name="ai-days"][value="${daysVal}"]`);
      if (daysRadio) { daysRadio.checked = true; }
    }
    if (project.reasoning) {
      const reasoningRadio = document.querySelector(`input[name="ai-reasoning"][value="${project.reasoning}"]`);
      if (reasoningRadio) { reasoningRadio.checked = true; }
    }

    // Restore to the right screen
    if (project.currentScreen === 'plan' && project.plan) {
      renderPlanScreen();
      switchScreen('plan', false);
    } else if (project.currentScreen === 'days' && project.days) {
      // If generation was interrupted, fix any 'generating' status
      const hadPending = project.days.some(d => d.status === 'generating');
      project.days.forEach(d => { if (d.status === 'generating') d.status = 'pending'; });
      await saveProject();
      renderDaysScreen();
      switchScreen('days', false);

      // If generation was interrupted, auto-resume for remaining pending days
      if (hadPending || project.days.some(d => d.status === 'pending')) {
        const hasReadyOrDone = project.days.some(d => d.status === 'ready' || d.status === 'implemented');
        if (hasReadyOrDone) {
          // Some days were already generated — resume generation for the rest
          generateAllDays();
        }
      }

      // Auto-expand the first actionable day
      const nextDayIdx = project.days.findIndex(d => d.status === 'ready');
      if (nextDayIdx >= 0) {
        requestAnimationFrame(() => {
          const header = document.querySelector(`.day-card-header[data-day="${nextDayIdx}"]`);
          if (header) header.click();
        });
      }
    }

    // Reposition all mode indicators after restore
    requestAnimationFrame(() => {
      document.querySelectorAll('.mode-options').forEach(c => positionModeIndicator(c, false));
    });
  }
})();

// ============================================================
// Save API keys / models with GSAP badge animation
// ============================================================
function bindSave(btnId, keyInputId, modelSelectId, keyStorageKey, modelStorageKey, badgeId) {
  document.getElementById(btnId).addEventListener('click', () => {
    const key   = document.getElementById(keyInputId).value.trim();
    const model = document.getElementById(modelSelectId).value;
    chrome.storage.local.set({ [keyStorageKey]: key, [modelStorageKey]: model }, () => {
      const badge = document.getElementById(badgeId);
      badge.style.display = 'block';
      gsap.fromTo(badge,
        { opacity: 0, scale: 0.8 },
        { opacity: 1, scale: 1, duration: 0.3, ease: 'back.out(1.7)' }
      );
      gsap.to(badge, {
        opacity: 0, scale: 0.8, duration: 0.25, delay: 2, ease: 'power2.in',
        onComplete: () => { badge.style.display = 'none'; },
      });
    });
  });
  // Auto-save model when changed
  document.getElementById(modelSelectId).addEventListener('change', () => {
    const model = document.getElementById(modelSelectId).value;
    chrome.storage.local.set({ [modelStorageKey]: model });
  });
}

bindSave('claude-save', 'claude-key', 'claude-model', 'claudeKey', 'claudeModel', 'claude-saved');
bindSave('gemini-save', 'gemini-key', 'gemini-model', 'geminiKey', 'geminiModel', 'gemini-saved');

// ============================================================
// Generate Mode: Prompts
// ============================================================
const PLANNING_SYSTEM_PROMPT = `You are a writing process planner. Your job is to design a realistic plan for how a specific human would write a piece of text over multiple sessions across multiple days.

You are NOT writing the text itself. You are planning the JOURNEY — who the writer is, how they think, what ideas they explore (including wrong turns and dead ends), and what each writing session looks and feels like.

This plan will be used to generate realistic Google Docs version history indistinguishable from a real human's writing process.

---

CREATE THE FOLLOWING:

1. PERSONA
A specific, believable writer. In 2-4 sentences, establish:
- Who they are and why they're writing this
- Their writing habits (brain-dumper? outliner? procrastinator? meticulous?)
- Their skill level and confidence with this topic

Make them feel like a real individual, not a stereotype.

2. STYLE & VOICE
How this specific person writes:
- Sentence structure tendencies
- Vocabulary level
- Recurring habits (dashes, parentheticals, sentence starters, filler phrases they overuse)
- What they DON'T do
- 2-3 example sentences you write that capture this person's voice precisely

These example sentences don't need to be about the topic — they just need to sound like this person.

3. TARGET LENGTH
Estimated word count range for the finished piece.

4. GHOST CONTENT
These are ideas, sentences, or whole paragraphs that will EXIST in the document temporarily but get DELETED before the final version. They are the wrong turns, tangents, and experiments that make real writing history feel human.

For each piece of ghost content:
- Briefly describe the idea/content
- When it appears and when it gets cut
- Why the writer cuts it (off-topic? wrong tone? redundant? too personal?)

Include at least 2 pieces of ghost content. Critically: these should be ideas the writer GENUINELY THINKS ARE GOOD when they write them. They're not obviously bad. They just don't survive the editing process.

5. DAY-BY-DAY ARC
For each day, in 2-3 sentences describe:
- Session type (brain dump, rereading & restructuring, expanding, tightening, polishing)
- The writer's mindset (excited, frustrated, fresh eyes, nitpicky)
- What broadly happens — decisions, not text
- What ghost content appears or gets cut, if any

PACING RULES:
- Day 1 is always the messiest and most generative — but it should only produce the FIRST paragraph/section. Do NOT write the whole piece on Day 1.
- Each subsequent day adds the next section/paragraph while also refining what exists. The piece grows gradually across days.
- Middle days involve adding new sections AND restructuring/editing earlier content
- The final day is minor polish — small word changes, fixing awkward phrasing, ensuring coherence
- For 1-2 day plans, compress this arc accordingly (Day 1 can cover more)
- The piece should NOT be complete after Day 1 — leave real work for later days
- Each day should have a meaningfully different purpose — don't repeat the same type of session

---

ANTI-AI VOICE — MANDATORY:
The persona you create must NOT write like a language model. When describing their style, explicitly exclude these AI patterns:
- No em dashes (—). Use commas, periods, or rewrite.
- No "It's not X — it's Y" rhetorical frames.
- No stacked transitions ("Furthermore," "Moreover," "Additionally," "In conclusion").
- No hollow intensifiers: "truly," "deeply," "incredibly," "remarkably," "profoundly."
- No "In today's [noun]..." or "In a world where..." openings.
- No "It is worth noting that..." or "It's important to remember..."
- No "the power of [abstract noun]" or "the beauty of [concept]."
- No "at its core" or "at the end of the day."
- No "serves as a testament/reminder" or "stands as a symbol."
- No "navigating [abstract concept]" as a metaphor.
- No "shedding light on" or "raises important questions about..."
- No "What makes [X] [adjective] is..." or "Perhaps most importantly,..."
- No "through the lens of" or "This speaks to a larger truth about..."
The example sentences in STYLE & VOICE must sound like a real human, not like AI output. If you catch yourself writing any of the above patterns, rewrite.

---

OUTPUT FORMAT:

PERSONA:
[2-4 sentences]

STYLE & VOICE:
[description + 2-3 example sentences]

TARGET LENGTH:
[word count range]

GHOST CONTENT:
1. [description] | Appears: Day X | Cut: Day Y | Reason: [reason]
2. [description] | Appears: Day X | Cut: Day Y | Reason: [reason]

DAY-BY-DAY ARC:
Day 1: [2-3 sentences]
Day 2: [2-3 sentences]
...`;

const DAY_GENERATION_SYSTEM_PROMPT = `You are simulating a real human writing session inside a Google Doc. You will output a sequence of STATES — snapshots of what the entire document looks like at different moments during this session.

These states will be diffed programmatically and executed as real keystrokes in Google Docs. The result must be indistinguishable from a real human's version history.

---

HOW REAL HUMANS WRITE:

Internalize these behaviors. They are what separates realistic output from obvious AI generation.

FORWARD MOMENTUM
- You are writing FORWARD. You do not know what the final version will look like. You are figuring it out as you go.
- Each state is a natural next step from the previous one. You are not reverse-engineering from a polished result.

UNEVEN PACING
- The amount of change between states should vary dramatically.
- Some transitions: an entire paragraph appears in a burst of fast writing.
- Some transitions: a single word changes.
- Some transitions: a large chunk gets deleted.
- Some transitions: text gets moved from one place to another.
- If every transition is roughly the same size, it looks fake. Vary it.

NATURAL IMPERFECTIONS
- Do NOT add intentional typos. The typing engine handles keystroke-level realism.
- Focus instead on content-level imperfections: awkward phrasing, weak word choices, sentences that don't quite land, ideas that are underdeveloped.
- These get improved through editing in later states — that's what makes the process feel real.

MID-SENTENCE ABANDONMENT
- Sometimes the writer starts a sentence, gets 3-8 words in, doesn't like where it's going, deletes it, and starts a completely different sentence.
- This may even happen mid-word.
- This is extremely common in real writing and almost never appears in AI-generated text. Use it where it feels natural.

FALSE STARTS AND WRONG TURNS
- Especially in early sessions, the writer may write something they genuinely think is good, then realize a few states later it doesn't work. They cut it.
- Content that gets introduced and later cut is what makes edit history feel alive.
- When writing this content, write it EARNESTLY — as if the writer truly believes it belongs. Don't write it as obviously bad filler.

THROAT-CLEARING
- Early drafts often start with a vague, unnecessary opening sentence: "When it comes to..." / "There are many reasons why..." / "It's important to consider..."
- This is the writer warming up. It gets deleted in a later state or session.

THINKING THROUGH THE KEYBOARD
- Early drafts often include: ideas in the order the writer THINKS of them (not the logical order), over-explanation of uncertain points, under-explanation of obvious points, two ideas crammed into one run-on sentence, abrupt or trailing-off endings.
- Later states fix these — but only some at a time, not all at once.

PLACEHOLDERS
- Real writers sometimes leave notes: "[add source here]", "[expand this]", "something something about X", "TODO: better transition".
- These get filled in or removed in later states.

CONFIDENCE GRADIENT
- Within a single state, some sentences come out clean and confident (the writer knew what they wanted to say) and others are wordy and fumbling (figuring it out on the page). This unevenness within a single state is very realistic.

REORGANIZATION
- Real editing isn't just adding and removing — it's moving. A sentence from paragraph 3 might get moved to the opening. Two paragraphs might swap order.

UNDO BEHAVIOR
- Sometimes the writer changes a word, then in the next state changes it back. Or rewrites a sentence, decides the original was better, and reverts. This is common and realistic.

PARAGRAPH STRUCTURE
- Early drafts may be one big block of text or have awkward paragraph breaks. The writer adds proper paragraph breaks during restructuring sessions, not during the initial brain dump.

---

SESSION-SPECIFIC BEHAVIOR:

DRAFTING SESSIONS (typically Day 1):
- Start from nothing. This is the messiest, most exploratory session.
- IMPORTANT: Day 1 should only cover the FIRST section/paragraph of the piece. Do NOT try to write the whole thing. A real writer focuses on getting the opening right before moving on. Later days handle later sections.
- Do NOT produce a clean rough draft. Produce what actually happens when someone sits down and starts writing — false starts, changes of direction, rewriting.
- Do NOT add intentional typos. Focus on content-level decisions: trying an idea, reconsidering it, rewriting.
- CRITICAL — YOU MUST FOLLOW THIS EXACT PATTERN (7-8 states):
  * State 1: Write 2-3 sentences. A first attempt at an opening.
  * State 2: Delete the sentences from State 1. The writer didn't like the direction. Write 3 new sentences — a fresh approach.
  * State 3: Change the wording on those three sentences. Swap a few words, rephrase one sentence.
  * State 4: Fix 1-2 small issues, improve sentence structure of 2 sentences, add 2 more sentences.
  * State 5: Rewrite the beginning in a slightly different way. Change something in the middle. Make it better.
  * State 6: Add 1 more sentence. Change the words on 2 sentences.
  * State 7: Polish — tighten phrasing across the whole thing, fix anything awkward.
- This is NOT optional. Every drafting session must follow this edit-heavy pattern. The result should be ONE solid paragraph, not a full essay.
- The key insight: MOST states should be EDITING existing text (rewriting, swapping words, restructuring), not adding new text. Only 2-3 states should add new sentences. The rest should be revision.

RESTRUCTURING SESSIONS (typically middle days):
- The writer opens the doc, rereads what they have, and reacts with fresh eyes.
- Big moves: cutting sections, reordering paragraphs, rewriting weak parts.
- This is where ghost content often dies — the writer rereads it and realizes it doesn't fit.
- 5-10 states.

EXPANDING SESSIONS (middle days):
- The writer identifies gaps — a missing point, a thin paragraph, a weak transition.
- Adds new content surgically. The new content might not be perfect on first try.
- 5-8 states.

POLISH SESSIONS (typically final day):
- Small precise changes: word swaps, cutting filler words, tightening sentences.
- Fixing that one awkward phrase that's been bugging the writer.
- Maybe changing something and changing it back (indecision on final wording).
- Very few new ideas. Content is set, just refining expression.
- 5-7 states.
- The final state of the final day should feel DONE — not perfect, but complete. The writer is satisfied enough to stop.

If this is NOT the final day, the last state should still have visible rough edges — things for future sessions to improve.

---

OUTPUT FORMAT:

=== STATE 1 ===
[Complete document text as it exists at this moment]

=== STATE 2 ===
[Complete document text as it exists at this moment]

...

=== STATE N ===
[Complete document text as it exists at this moment]

ANTI-AI VOICE — MANDATORY:
Cut anything that sounds pre-packaged. Real student/human writing does NOT contain these patterns:
- No em dashes (—). Use commas, periods, or rewrite the sentence.
- No "It's not X — it's Y" / "It's not just X, it's Y" rhetorical frames.
- No stacked transitions ("Furthermore," "Moreover," "Additionally," "In conclusion").
- No "This isn't just A, it's B" or "This goes beyond A to B" escalation patterns.
- No hollow intensifiers: "truly," "deeply," "incredibly," "remarkably," "profoundly."
- No "In today's [noun]..." or "In a world where..." openings.
- No "It is worth noting that..." or "It's important to remember..." throat-clearing.
- No triple-structure lists: "X, Y, and Z" used rhetorically more than once.
- No "the power of [abstract noun]" or "the beauty of [concept]."
- No "at its core" or "at the end of the day."
- No "serves as a testament/reminder" or "stands as a symbol."
- No "navigating [abstract concept]" as a metaphor.
- No "shedding light on" or "shining a light on."
- No "the intersection of X and Y."
- No "it's easy to forget that..." or "we often overlook..."
- No "raises important questions about..."
- No sentences that start with "What makes [X] [adjective] is..."
- No "Perhaps most importantly,..." or "What's particularly striking is..."
- No "This speaks to a larger truth about..."
- No "lens" as metaphor ("through the lens of").
Write like a real person, not like a language model.

CRITICAL RULES:
- Every state must contain the FULL document text. No shortcuts like "[rest unchanged]". The states will be diffed programmatically.
- Separate paragraphs with blank lines consistently across all states.
- Do not include commentary, notes, or explanation outside the states. Output ONLY the states.`;

function buildPlanUserMessage(topic, numDays, context) {
  let msg = `Topic: ${topic}\nNumber of days: ${numDays || 'Auto (decide based on the topic and scope)'}`;
  if (context) msg += `\nAdditional context: ${context}`;
  return msg;
}

function buildDayUserMessage(plan, dayNumber, totalDays, dayArc, topic, context, previousFinalState) {
  return `PERSONA:\n${plan.persona}\n\nSTYLE & VOICE:\n${plan.style}\n\nTARGET LENGTH:\n${plan.targetLength}\n\nSESSION INFO:\nDay ${dayNumber} of ${totalDays}\n\nTODAY'S SESSION:\n${dayArc}\n\nCONTENT DIRECTIONS:\nTopic: ${topic}${context ? '\n' + context : ''}\n\nCURRENT DOCUMENT:\n${previousFinalState || 'Empty — this is the first session.'}`;
}

// ============================================================
// Generate Mode: Parsing
// ============================================================
function parsePlanOutput(raw) {
  const sections = {};
  const sectionNames = ['PERSONA', 'STYLE & VOICE', 'TARGET LENGTH', 'GHOST CONTENT', 'DAY-BY-DAY ARC'];

  for (let i = 0; i < sectionNames.length; i++) {
    const name = sectionNames[i];
    const regex = new RegExp(`${name.replace(/[.*+?^${}()|[\]\\]/g, '\\$&')}:\\s*\\n?`, 'i');
    const match = raw.match(regex);
    if (!match) continue;

    const startIdx = match.index + match[0].length;
    // Find the start of the next section
    let endIdx = raw.length;
    for (let j = i + 1; j < sectionNames.length; j++) {
      const nextRegex = new RegExp(`\\n${sectionNames[j].replace(/[.*+?^${}()|[\]\\]/g, '\\$&')}:`, 'i');
      const nextMatch = raw.slice(startIdx).match(nextRegex);
      if (nextMatch) {
        endIdx = startIdx + nextMatch.index;
        break;
      }
    }
    sections[name] = raw.slice(startIdx, endIdx).trim();
  }

  // Parse day arcs into array
  const dayArcsRaw = sections['DAY-BY-DAY ARC'] || '';
  const dayArcs = [];
  const dayRegex = /Day\s+(\d+)\s*:\s*/gi;
  let dayMatch;
  const dayPositions = [];
  while ((dayMatch = dayRegex.exec(dayArcsRaw)) !== null) {
    dayPositions.push({ num: parseInt(dayMatch[1]), start: dayMatch.index + dayMatch[0].length });
  }
  for (let i = 0; i < dayPositions.length; i++) {
    const start = dayPositions[i].start;
    const end = i + 1 < dayPositions.length ? dayPositions[i + 1].start - (dayArcsRaw.slice(dayPositions[i + 1].start).match(/^/) ? 0 : 0) : dayArcsRaw.length;
    // Find the end of this day's text (next "Day N:" or end of string)
    const endIdx = i + 1 < dayPositions.length
      ? dayArcsRaw.lastIndexOf('\n', dayPositions[i + 1].start - 1) || dayPositions[i + 1].start
      : dayArcsRaw.length;
    dayArcs.push(dayArcsRaw.slice(start, endIdx).trim());
  }

  return {
    raw,
    persona: sections['PERSONA'] || '',
    style: sections['STYLE & VOICE'] || '',
    targetLength: sections['TARGET LENGTH'] || '',
    ghostContent: sections['GHOST CONTENT'] || '',
    dayArcs,
  };
}

function parseDayOutput(raw) {
  const stateRegex = /^=== STATE \d+ ===/gm;
  const matches = [...raw.matchAll(stateRegex)];
  if (matches.length === 0) return [];

  const states = [];
  for (let i = 0; i < matches.length; i++) {
    const start = matches[i].index + matches[i][0].length;
    const end = i + 1 < matches.length ? matches[i + 1].index : raw.length;
    const stateText = raw.slice(start, end).trim();
    if (stateText) states.push(stateText);
  }
  return states;
}

// ============================================================
// Generate Mode: Project State
// ============================================================
let currentProject = null;

async function saveProject() {
  if (!currentProject) return;
  await new Promise(resolve => chrome.storage.local.set({ mythosProject: currentProject }, resolve));
}

async function loadProject() {
  const data = await chromeGet(['mythosProject']);
  currentProject = data.mythosProject || null;
  return currentProject;
}

async function clearProject() {
  currentProject = null;
  await new Promise(resolve => chrome.storage.local.remove('mythosProject', resolve));
}

// ============================================================
// Generate Mode: Screen Management
// ============================================================
function switchScreen(screenName, animate = true) {
  const screens = document.querySelectorAll('#tab-ai .ai-screen');
  const target = document.getElementById('ai-screen-' + screenName);
  if (!target) return;

  const oldScreen = document.querySelector('#tab-ai .ai-screen.active');

  if (oldScreen && oldScreen !== target && animate) {
    gsap.to(oldScreen, {
      opacity: 0, y: 12, duration: 0.2, ease: 'power2.in',
      onComplete: () => {
        oldScreen.classList.remove('active');
        gsap.set(oldScreen, { opacity: 1, y: 0 });
        target.classList.add('active');
        gsap.set(target, { opacity: 0, y: -12 });
        gsap.to(target, { opacity: 1, y: 0, duration: 0.25, ease: 'power2.out' });

        // Stagger cards in the new screen
        const cards = target.querySelectorAll('.card, .day-card');
        if (cards.length) {
          gsap.fromTo(cards,
            { opacity: 0, y: 16 },
            { opacity: 1, y: 0, duration: 0.35, stagger: 0.08, ease: 'power3.out', delay: 0.05 }
          );
        }

        // Position mode indicators
        target.querySelectorAll('.mode-options').forEach(c => positionModeIndicator(c, false));
      },
    });
  } else {
    screens.forEach(s => s.classList.remove('active'));
    target.classList.add('active');
    target.querySelectorAll('.mode-options').forEach(c => positionModeIndicator(c, false));
  }

  if (currentProject) {
    currentProject.currentScreen = screenName;
    saveProject();
  }
}

// ============================================================
// Generate Mode: Screen 1 — Plan Generation
// ============================================================
document.getElementById('ai-gen-plan-btn').addEventListener('click', async () => {
  const genBtn    = document.getElementById('ai-gen-plan-btn');
  const statusEl  = document.getElementById('ai-input-status');

  // Cancel if currently streaming
  if (streamAbortController) {
    streamAbortController.abort();
    streamAbortController = null;
    genBtn.textContent = 'Generate Plan';
    setStatus(statusEl, 'Cancelled.', '');
    return;
  }

  const topic     = document.getElementById('ai-topic').value.trim();
  const provider  = document.getElementById('ai-provider').value;
  const reasoning = getSelectedMode('ai-reasoning');
  const daysVal   = getSelectedMode('ai-days');
  const context   = document.getElementById('ai-context').value.trim();

  if (!topic) {
    setStatus(statusEl, 'Enter a topic first.', 'error');
    return;
  }

  const stored = await chromeGet(['claudeKey', 'claudeModel', 'geminiKey', 'geminiModel']);
  const apiKey = provider === 'claude' ? stored.claudeKey : stored.geminiKey;
  const model  = provider === 'claude' ? stored.claudeModel : stored.geminiModel;

  if (!apiKey) {
    setStatus(statusEl, `No ${provider === 'claude' ? 'Claude' : 'Gemini'} API key saved. Go to Settings.`, 'error');
    return;
  }

  // Setup streaming
  streamAbortController = new AbortController();
  genBtn.textContent = 'Cancel';
  setStatus(statusEl, 'Generating plan...', '');

  const hasReasoning = reasoning !== 'none';
  const numDays = daysVal === 'auto' ? null : parseInt(daysVal);
  const userMessage = buildPlanUserMessage(topic, numDays, context);
  let rawPlan = '';
  let receivedText = false;

  const streamFn = provider === 'claude' ? streamClaude : streamGemini;

  // Switch to plan screen immediately with loading state
  const planOverlay = document.getElementById('plan-thinking-overlay');
  const planThinkingLabel = document.getElementById('plan-thinking-label');
  const planThinkingContent = document.getElementById('plan-thinking-content');
  const planThinkingBody = document.getElementById('plan-thinking-body');
  const planBtnRow = document.getElementById('plan-btn-row');

  // Clear plan sections and show loading shimmer
  ['plan-persona', 'plan-style', 'plan-length', 'plan-ghost'].forEach(id => {
    const el = document.getElementById(id);
    el.textContent = '';
    el.classList.add('loading');
  });
  document.getElementById('plan-arcs').innerHTML = '';
  planBtnRow.style.display = 'none';

  // Show thinking overlay if reasoning is enabled
  if (hasReasoning) {
    planThinkingContent.textContent = '';
    gsap.set(planThinkingContent, { y: 0 });
    planThinkingLabel.textContent = 'Thinking';
    planThinkingLabel.classList.add('thinking-label-streaming');
    planOverlay.style.display = 'block';
    gsap.fromTo(planOverlay, { opacity: 0, y: -10 }, { opacity: 1, y: 0, duration: 0.3, ease: 'power2.out' });
  } else {
    planOverlay.style.display = 'none';
  }

  switchScreen('plan');

  try {
    await streamFn({
      apiKey, model,
      prompt: userMessage,
      system: PLANNING_SYSTEM_PROMPT,
      reasoning,
      maxTokens: 16000,
      onThinking: (chunk) => {
        if (hasReasoning) {
          planThinkingContent.textContent += chunk;
          const overflow = planThinkingContent.scrollHeight - planThinkingBody.clientHeight;
          if (overflow > 0) {
            gsap.to(planThinkingContent, { y: -overflow, duration: 0.3, ease: 'power1.out', overwrite: true });
          }
        }
      },
      onText: (chunk) => {
        if (!receivedText && hasReasoning) {
          receivedText = true;
          planThinkingLabel.textContent = 'Thinking — done';
          planThinkingLabel.classList.remove('thinking-label-streaming');
          // Fade out thinking overlay
          gsap.to(planOverlay, { opacity: 0, y: -10, duration: 0.5, delay: 0.8, ease: 'power2.in', onComplete: () => {
            planOverlay.style.display = 'none';
          }});
        }
        rawPlan += chunk;
        // Progressively parse and fill plan sections
        progressivePlanFill(rawPlan);
      },
      signal: streamAbortController.signal,
    });

    if (hasReasoning && !receivedText) {
      planThinkingLabel.textContent = 'Thinking — done';
      planThinkingLabel.classList.remove('thinking-label-streaming');
      gsap.to(planOverlay, { opacity: 0, y: -10, duration: 0.5, ease: 'power2.in', onComplete: () => {
        planOverlay.style.display = 'none';
      }});
    }

    // Parse the plan
    const plan = parsePlanOutput(rawPlan);
    if (!plan.persona && !plan.style) {
      setStatus(statusEl, 'Plan format not recognized. Try regenerating.', 'error');
      streamAbortController = null;
      genBtn.textContent = 'Generate Plan';
      switchScreen('input');
      return;
    }

    // Final render with all sections
    renderPlanScreen();
    // Remove loading shimmer
    ['plan-persona', 'plan-style', 'plan-length', 'plan-ghost'].forEach(id => {
      document.getElementById(id).classList.remove('loading');
    });
    // Show buttons with animation
    planBtnRow.style.display = 'flex';
    gsap.fromTo(planBtnRow, { opacity: 0, y: 10 }, { opacity: 1, y: 0, duration: 0.3, ease: 'power2.out' });

    // Determine number of days
    const finalDays = numDays || plan.dayArcs.length || 3;

    // Get the current doc URL for same-doc detection on reopen
    let docUrl = '';
    try {
      const tab = await getActiveTab();
      if (tab && tab.url && tab.url.includes('docs.google.com/document')) {
        docUrl = tab.url.split('?')[0];
      }
    } catch { /* ignore */ }

    currentProject = {
      id: String(Date.now()),
      topic,
      numDays: finalDays,
      context,
      provider,
      reasoning,
      docUrl,
      plan,
      days: Array.from({ length: finalDays }, (_, i) => ({
        dayNumber: i + 1,
        status: 'pending',
        states: [],
        implementedStateIndex: -1,
      })),
      currentScreen: 'plan',
    };
    await saveProject();
    setStatus(statusEl, '', '');

  } catch (err) {
    if (err.name === 'AbortError') {
      setStatus(statusEl, 'Cancelled.', '');
      switchScreen('input');
    } else {
      setStatus(statusEl, 'Error: ' + err.message, 'error');
      switchScreen('input');
    }
    // Clean up overlay
    planOverlay.style.display = 'none';
    planThinkingLabel.classList.remove('thinking-label-streaming');
  } finally {
    streamAbortController = null;
    genBtn.textContent = 'Generate Plan';
  }
});

// ============================================================
// Generate Mode: Screen 2 — Plan Review
// ============================================================
// Progressively fill plan sections as streaming text arrives
function progressivePlanFill(rawSoFar) {
  const sections = [
    { key: 'PERSONA', elId: 'plan-persona' },
    { key: 'STYLE & VOICE', elId: 'plan-style' },
    { key: 'TARGET LENGTH', elId: 'plan-length' },
    { key: 'GHOST CONTENT', elId: 'plan-ghost' },
  ];

  for (let i = 0; i < sections.length; i++) {
    const { key, elId } = sections[i];
    const regex = new RegExp(`${key.replace(/[.*+?^${}()|[\]\\]/g, '\\$&')}:\\s*\\n?`, 'i');
    const match = rawSoFar.match(regex);
    if (!match) continue;

    const startIdx = match.index + match[0].length;
    // Find end: next section header or end of text
    let endIdx = rawSoFar.length;
    for (let j = i + 1; j < sections.length; j++) {
      const nextRegex = new RegExp(`\\n${sections[j].key.replace(/[.*+?^${}()|[\]\\]/g, '\\$&')}:`, 'i');
      const nextMatch = rawSoFar.slice(startIdx).match(nextRegex);
      if (nextMatch) { endIdx = startIdx + nextMatch.index; break; }
    }
    // Also check DAY-BY-DAY ARC
    const arcRegex = /\nDAY-BY-DAY ARC:/i;
    const arcMatch = rawSoFar.slice(startIdx).match(arcRegex);
    if (arcMatch && startIdx + arcMatch.index < endIdx) {
      endIdx = startIdx + arcMatch.index;
    }

    const text = rawSoFar.slice(startIdx, endIdx).trim();
    const el = document.getElementById(elId);
    if (text && el) {
      el.textContent = text;
      el.classList.remove('loading');
    }
  }

  // Progressive day arcs
  const arcHeaderRegex = /DAY-BY-DAY ARC:\s*\n?/i;
  const arcHeaderMatch = rawSoFar.match(arcHeaderRegex);
  if (arcHeaderMatch) {
    const arcStart = arcHeaderMatch.index + arcHeaderMatch[0].length;
    const arcText = rawSoFar.slice(arcStart).trim();
    const dayRegex = /Day\s+(\d+)\s*:\s*/gi;
    let dayMatch;
    const dayPositions = [];
    while ((dayMatch = dayRegex.exec(arcText)) !== null) {
      dayPositions.push({ num: parseInt(dayMatch[1]), start: dayMatch.index + dayMatch[0].length });
    }
    if (dayPositions.length > 0) {
      const arcsList = document.getElementById('plan-arcs');
      arcsList.innerHTML = '';
      for (let i = 0; i < dayPositions.length; i++) {
        const start = dayPositions[i].start;
        const end = i + 1 < dayPositions.length
          ? arcText.lastIndexOf('\n', dayPositions[i + 1].start - 1) || dayPositions[i + 1].start
          : arcText.length;
        const arcContent = arcText.slice(start, end).trim();
        const li = document.createElement('li');
        li.className = 'day-arc-item';
        li.innerHTML = `<strong>Day ${dayPositions[i].num}:</strong> ${arcContent}`;
        arcsList.appendChild(li);
      }
    }
  }
}

function renderPlanScreen() {
  if (!currentProject || !currentProject.plan) return;
  const p = currentProject.plan;

  const personaEl = document.getElementById('plan-persona');
  const styleEl = document.getElementById('plan-style');
  const lengthEl = document.getElementById('plan-length');
  const ghostEl = document.getElementById('plan-ghost');

  personaEl.textContent = p.persona;
  personaEl.classList.remove('loading');
  styleEl.textContent = p.style;
  styleEl.classList.remove('loading');
  lengthEl.textContent = p.targetLength;
  lengthEl.classList.remove('loading');
  ghostEl.textContent = p.ghostContent;
  ghostEl.classList.remove('loading');

  const arcsList = document.getElementById('plan-arcs');
  arcsList.innerHTML = '';
  p.dayArcs.forEach((arc, i) => {
    const li = document.createElement('li');
    li.className = 'day-arc-item';
    li.innerHTML = `<strong>Day ${i + 1}:</strong> ${arc}`;
    arcsList.appendChild(li);
  });

  // Ensure button row is visible (may be hidden during progressive fill)
  const btnRow = document.getElementById('plan-btn-row');
  btnRow.style.display = 'flex';
  btnRow.style.opacity = '1';

  // Ensure thinking overlay is hidden
  document.getElementById('plan-thinking-overlay').style.display = 'none';
}

// Back to input (also cancels ongoing generation if any)
document.getElementById('plan-back-btn').addEventListener('click', () => {
  if (streamAbortController) {
    streamAbortController.abort();
    streamAbortController = null;
  }
  document.getElementById('plan-thinking-overlay').style.display = 'none';
  document.getElementById('ai-gen-plan-btn').textContent = 'Generate Plan';
  switchScreen('input');
});

// Edit Plan — show edit input row
document.getElementById('plan-edit-btn').addEventListener('click', () => {
  const editRow = document.getElementById('plan-edit-row');
  const btnRow = document.getElementById('plan-btn-row');
  editRow.style.display = 'block';
  btnRow.style.display = 'none';
  document.getElementById('plan-edit-input').value = '';
  document.getElementById('plan-edit-input').focus();
});

// Cancel edit
document.getElementById('plan-edit-cancel').addEventListener('click', () => {
  document.getElementById('plan-edit-row').style.display = 'none';
  document.getElementById('plan-btn-row').style.display = 'flex';
});

// Apply edit — send original plan + user instructions to AI for revision
document.getElementById('plan-edit-submit').addEventListener('click', async () => {
  const editInput = document.getElementById('plan-edit-input');
  const editRow = document.getElementById('plan-edit-row');
  const instructions = editInput.value.trim();
  if (!instructions) return;

  const statusEl = document.getElementById('plan-status');
  if (!currentProject || !currentProject.plan) return;

  const stored = await chromeGet(['claudeKey', 'claudeModel', 'geminiKey', 'geminiModel']);
  const provider = currentProject.provider;
  const apiKey = provider === 'claude' ? stored.claudeKey : stored.geminiKey;
  const model  = provider === 'claude' ? stored.claudeModel : stored.geminiModel;
  const streamFn = provider === 'claude' ? streamClaude : streamGemini;
  const reasoning = currentProject.reasoning || 'none';
  const hasReasoning = reasoning !== 'none';

  if (!apiKey) {
    setStatus(statusEl, `No API key. Go to Settings.`, 'error');
    return;
  }

  // Hide edit row, show loading
  editRow.style.display = 'none';

  // Show shimmer on plan sections
  ['plan-persona', 'plan-style', 'plan-length', 'plan-ghost'].forEach(id => {
    const el = document.getElementById(id);
    el.classList.add('loading');
  });

  const planBtnRow = document.getElementById('plan-btn-row');
  planBtnRow.style.display = 'none';

  // Show thinking overlay if reasoning enabled
  const planOverlay = document.getElementById('plan-thinking-overlay');
  const planThinkingLabel = document.getElementById('plan-thinking-label');
  const planThinkingContent = document.getElementById('plan-thinking-content');
  const planThinkingBody = document.getElementById('plan-thinking-body');

  if (hasReasoning) {
    planThinkingContent.textContent = '';
    gsap.set(planThinkingContent, { y: 0 });
    planThinkingLabel.textContent = 'Thinking';
    planThinkingLabel.classList.add('thinking-label-streaming');
    planOverlay.style.display = 'block';
    gsap.fromTo(planOverlay, { opacity: 0, y: -10 }, { opacity: 1, y: 0, duration: 0.3, ease: 'power2.out' });
  }

  streamAbortController = new AbortController();
  setStatus(statusEl, 'Revising plan...', '');

  const editSystemPrompt = PLANNING_SYSTEM_PROMPT + `\n\n---\n\nYou are REVISING an existing plan. The user wants changes. Output the COMPLETE revised plan in the exact same format (PERSONA, STYLE & VOICE, TARGET LENGTH, GHOST CONTENT, DAY-BY-DAY ARC). Do not output explanations — only the revised plan.`;

  const editUserMessage = `CURRENT PLAN:\n${currentProject.plan.raw}\n\n---\n\nREQUESTED CHANGES:\n${instructions}\n\nPlease revise the plan according to the requested changes. Output the full revised plan.`;

  let rawPlan = '';
  let receivedText = false;

  try {
    await streamFn({
      apiKey, model,
      prompt: editUserMessage,
      system: editSystemPrompt,
      reasoning,
      maxTokens: 16000,
      onThinking: (chunk) => {
        if (hasReasoning) {
          planThinkingContent.textContent += chunk;
          const overflow = planThinkingContent.scrollHeight - planThinkingBody.clientHeight;
          if (overflow > 0) {
            gsap.to(planThinkingContent, { y: -overflow, duration: 0.3, ease: 'power1.out', overwrite: true });
          }
        }
      },
      onText: (chunk) => {
        if (!receivedText && hasReasoning) {
          receivedText = true;
          planThinkingLabel.textContent = 'Thinking — done';
          planThinkingLabel.classList.remove('thinking-label-streaming');
          gsap.to(planOverlay, { opacity: 0, y: -10, duration: 0.5, delay: 0.8, ease: 'power2.in', onComplete: () => {
            planOverlay.style.display = 'none';
          }});
        }
        rawPlan += chunk;
        progressivePlanFill(rawPlan);
      },
      signal: streamAbortController.signal,
    });

    if (hasReasoning && !receivedText) {
      planThinkingLabel.textContent = 'Thinking — done';
      planThinkingLabel.classList.remove('thinking-label-streaming');
      gsap.to(planOverlay, { opacity: 0, y: -10, duration: 0.5, ease: 'power2.in', onComplete: () => {
        planOverlay.style.display = 'none';
      }});
    }

    const plan = parsePlanOutput(rawPlan);
    if (!plan.persona && !plan.style) {
      setStatus(statusEl, 'Revised plan format not recognized. Try again.', 'error');
      // Restore original plan display
      renderPlanScreen();
      planBtnRow.style.display = 'flex';
      streamAbortController = null;
      return;
    }

    // Update current project with revised plan
    currentProject.plan = plan;
    const finalDays = currentProject.numDays || plan.dayArcs.length || 3;
    currentProject.numDays = finalDays;
    // Reset days since plan changed
    currentProject.days = Array.from({ length: finalDays }, (_, i) => ({
      dayNumber: i + 1,
      status: 'pending',
      states: [],
      implementedStateIndex: -1,
    }));
    await saveProject();

    renderPlanScreen();
    ['plan-persona', 'plan-style', 'plan-length', 'plan-ghost'].forEach(id => {
      document.getElementById(id).classList.remove('loading');
    });
    planBtnRow.style.display = 'flex';
    gsap.fromTo(planBtnRow, { opacity: 0, y: 10 }, { opacity: 1, y: 0, duration: 0.3, ease: 'power2.out' });

    setStatus(statusEl, 'Plan updated!', 'ok');
  } catch (err) {
    if (err.name === 'AbortError') {
      setStatus(statusEl, 'Edit cancelled.', '');
    } else {
      setStatus(statusEl, 'Error: ' + (err.message || 'Unknown'), 'error');
    }
    // Restore plan display
    renderPlanScreen();
    planBtnRow.style.display = 'flex';
    planOverlay.style.display = 'none';
  } finally {
    streamAbortController = null;
  }
});

// Approve & Generate
document.getElementById('plan-approve-btn').addEventListener('click', async () => {
  switchScreen('days');
  await generateAllDays();
});

// ============================================================
// Generate Mode: Screen 3 — Day Generation & View
// ============================================================
async function generateAllDays() {
  if (!currentProject) return;

  const statusEl = document.getElementById('days-gen-status');
  const progressEl = document.getElementById('days-progress');
  const progressBar = document.getElementById('days-progress-bar');

  progressEl.style.display = 'block';
  const totalDays = currentProject.numDays;

  const stored = await chromeGet(['claudeKey', 'claudeModel', 'geminiKey', 'geminiModel']);
  const provider = currentProject.provider;
  const apiKey = provider === 'claude' ? stored.claudeKey : stored.geminiKey;
  const model  = provider === 'claude' ? stored.claudeModel : stored.geminiModel;
  const streamFn = provider === 'claude' ? streamClaude : streamGemini;
  const reasoning = currentProject.reasoning || 'none';

  for (let i = 0; i < totalDays; i++) {
    const day = currentProject.days[i];
    if (day.status === 'ready' || day.status === 'implemented') {
      // Already generated, skip
      progressBar.style.width = `${((i + 1) / totalDays) * 100}%`;
      continue;
    }

    day.status = 'generating';
    currentProject.generatingDay = i + 1;
    renderDaysScreen();
    setWaveStatus(statusEl, `Generating Day ${i + 1} of ${totalDays}...`);
    progressBar.style.width = `${(i / totalDays) * 100}%`;

    // Auto-expand the generating day's card so user can see the stream
    requestAnimationFrame(() => {
      const genHeader = document.querySelector(`.day-card-header[data-day="${i}"]`);
      const genBody = document.querySelector(`.day-card-body[data-day="${i}"]`);
      if (genHeader && genBody && !genBody.classList.contains('expanded')) {
        genHeader.click();
      }
    });

    // Get previous day's final state
    let previousFinalState = '';
    if (i > 0 && currentProject.days[i - 1].states.length > 0) {
      const prevStates = currentProject.days[i - 1].states;
      previousFinalState = prevStates[prevStates.length - 1];
    }

    const dayArc = currentProject.plan.dayArcs[i] || '';
    const userMessage = buildDayUserMessage(
      currentProject.plan, i + 1, totalDays, dayArc,
      currentProject.topic, currentProject.context, previousFinalState
    );

    let rawDay = '';
    streamAbortController = new AbortController();

    try {
      await streamFn({
        apiKey, model,
        prompt: userMessage,
        system: DAY_GENERATION_SYSTEM_PROMPT,
        reasoning,
        maxTokens: 32000,
        onThinking: () => {},
        onText: (chunk) => {
          rawDay += chunk;
          // Stream into the day card's preview area
          const streamEl = document.querySelector(`.day-stream-scroll[data-day="${i}"]`);
          if (streamEl) {
            streamEl.textContent = rawDay;
            // Auto-scroll to bottom
            const windowEl = streamEl.parentElement;
            if (windowEl) {
              const overflow = streamEl.scrollHeight - windowEl.clientHeight;
              if (overflow > 0) {
                gsap.to(streamEl, { y: -overflow, duration: 0.3, ease: 'power1.out', overwrite: true });
              }
            }
          }
        },
        signal: streamAbortController.signal,
      });

      const states = parseDayOutput(rawDay);
      day.states = states;
      day.status = states.length > 0 ? 'ready' : 'pending';
      currentProject.generatingDay = null;
      await saveProject();
      renderDaysScreen();
      progressBar.style.width = `${((i + 1) / totalDays) * 100}%`;

      // Rate limit delay between calls
      if (i < totalDays - 1) await new Promise(r => setTimeout(r, 2000));

    } catch (err) {
      streamAbortController = null;
      currentProject.generatingDay = null;
      if (err.name === 'AbortError') {
        day.status = 'pending';
        setStatus(statusEl, 'Generation cancelled.', '');
        await saveProject();
        renderDaysScreen();
        return;
      } else {
        day.status = 'pending';
        setStatus(statusEl, `Error generating Day ${i + 1}: ${err.message}`, 'error');
        await saveProject();
        renderDaysScreen();
        return;
      }
    }
  }

  streamAbortController = null;
  progressEl.style.display = 'none';
  setStatus(statusEl, 'All days generated! Implement them one at a time.', 'ok');
  // Auto-fade the completion message after 3 seconds
  setTimeout(() => {
    if (statusEl.textContent.includes('All days generated')) {
      gsap.to(statusEl, { opacity: 0, y: -4, duration: 0.5, ease: 'power2.in', onComplete: () => {
        statusEl.textContent = '';
        statusEl.style.opacity = '';
        statusEl.style.transform = '';
      }});
    }
  }, 3000);
}

function renderDaysScreen() {
  if (!currentProject) return;
  const container = document.getElementById('days-container');
  container.innerHTML = '';

  currentProject.days.forEach((day, i) => {
    const card = document.createElement('div');
    card.className = 'day-card';
    card.id = `day-card-${i}`;

    // Badge
    let badgeClass = 'badge-pending';
    let badgeText = 'Pending';
    if (day.status === 'generating') { badgeClass = 'badge-generating'; badgeText = 'Generating...'; }
    else if (day.status === 'ready') { badgeClass = 'badge-ready'; badgeText = `${day.states.length} states`; }
    else if (day.status === 'implemented') { badgeClass = 'badge-implemented'; badgeText = 'Done'; }

    // Show the final state text (last state) if available, otherwise the arc description
    const finalState = day.states.length > 0 ? day.states[day.states.length - 1] : '';
    const dayArc = currentProject.plan.dayArcs[i] || '';
    const previewText = finalState
      ? escapeHTML(finalState.length > 600 ? finalState.slice(0, 600) + '…' : finalState)
      : escapeHTML(dayArc);
    const previewLabel = finalState ? 'Final draft' : 'Plan';

    card.innerHTML = `
      <div class="day-card-header" data-day="${i}">
        <div class="day-card-header-left">
          <span class="day-card-title">Day ${day.dayNumber}</span>
          <span class="status-badge ${badgeClass}">${badgeText}</span>
        </div>
        <svg class="day-card-chevron" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><polyline points="6 9 12 15 18 9"/></svg>
      </div>
      <div class="day-card-body" data-day="${i}">
        ${day.status === 'generating' ? `
          <div class="day-gen-stream" data-day="${i}">
            <div class="thinking-stream-label thinking-label-streaming">Generating</div>
            <div class="thinking-stream-window day-stream-window">
              <div class="thinking-stream-fade thinking-stream-fade-top"></div>
              <div class="day-stream-scroll" data-day="${i}"></div>
              <div class="thinking-stream-fade thinking-stream-fade-bottom"></div>
            </div>
          </div>
        ` : `
          <div class="day-card-preview-label">${previewLabel}</div>
          <div class="day-card-meta">${previewText}</div>
        `}
        ${day.status === 'ready' || day.status === 'implemented' ? `
          <div class="mode-row">
            <span class="mode-label">Speed</span>
            <div class="mode-options">
              <div class="mode-indicator"></div>
              <label><input type="radio" name="day-${i}-mode" value="fast" checked><span>Fast</span></label>
              <label><input type="radio" name="day-${i}-mode" value="medium"><span>Medium</span></label>
              <label><input type="radio" name="day-${i}-mode" value="human"><span>Human</span></label>
            </div>
          </div>
          <button class="btn-primary day-implement-btn" data-day="${i}" ${day.status === 'implemented' ? 'disabled' : ''}>
            ${day.status === 'implemented' ? 'Implemented' : `Implement Day ${day.dayNumber}`}
          </button>
          <div class="ctrl-row day-ctrl-row" data-day="${i}" style="display:none;">
            <button class="btn-secondary day-pause-btn" data-day="${i}" disabled>Pause</button>
            <button class="btn-secondary btn-stop day-stop-btn" data-day="${i}" disabled>Stop</button>
          </div>
          <div class="status day-impl-status" data-day="${i}"></div>
        ` : ''}
      </div>
    `;
    container.appendChild(card);
  });

  // Bind expand/collapse (accordion: only one open at a time)
  container.querySelectorAll('.day-card-header').forEach(header => {
    header.addEventListener('click', () => {
      const dayIdx = header.dataset.day;
      const body = container.querySelector(`.day-card-body[data-day="${dayIdx}"]`);
      const chevron = header.querySelector('.day-card-chevron');
      const isExpanding = !body.classList.contains('expanded');

      // Don't collapse the card that's currently being implemented
      if (!isExpanding && implementingDayIdx !== null && parseInt(dayIdx) === implementingDayIdx) return;

      // Collapse all other day cards first (accordion) — skip implementing day
      if (isExpanding) {
        container.querySelectorAll('.day-card-body.expanded').forEach(otherBody => {
          if (otherBody === body) return;
          // Don't collapse the card that's currently implementing
          if (implementingDayIdx !== null && parseInt(otherBody.dataset.day) === implementingDayIdx) return;
          const otherChevron = otherBody.closest('.day-card').querySelector('.day-card-chevron');
          gsap.to(otherBody, { height: 0, opacity: 0, duration: 0.25, ease: 'power2.in', onComplete: () => {
            otherBody.classList.remove('expanded');
            otherBody.style.display = 'none';
            otherBody.style.height = '';
            otherBody.style.opacity = '';
          }});
          if (otherChevron) otherChevron.classList.remove('expanded');
        });
      }

      if (isExpanding) {
        body.style.display = 'flex';
        body.classList.add('expanded');
        gsap.fromTo(body, { height: 0, opacity: 0 }, { height: 'auto', opacity: 1, duration: 0.3, ease: 'power2.out' });
        chevron.classList.add('expanded');
        // Position mode indicators after body is visible
        requestAnimationFrame(() => {
          body.querySelectorAll('.mode-options').forEach(c => positionModeIndicator(c, false));
        });
      } else {
        gsap.to(body, { height: 0, opacity: 0, duration: 0.25, ease: 'power2.in', onComplete: () => {
          body.classList.remove('expanded');
          body.style.display = 'none';
          body.style.height = '';
          body.style.opacity = '';
        }});
        chevron.classList.remove('expanded');
      }
    });
  });

  // Bind implement buttons
  container.querySelectorAll('.day-implement-btn').forEach(btn => {
    btn.addEventListener('click', () => implementDay(parseInt(btn.dataset.day)));
  });

  // Bind mode indicator animations for dynamically created day cards
  container.querySelectorAll('.mode-options').forEach((modeContainer) => {
    requestAnimationFrame(() => positionModeIndicator(modeContainer, false));
    modeContainer.querySelectorAll('input[type="radio"]').forEach((radio) => {
      radio.addEventListener('change', () => positionModeIndicator(modeContainer, true));
    });
  });

  // Bind pause/stop buttons
  container.querySelectorAll('.day-pause-btn').forEach(btn => {
    bindPause(btn);
  });
  container.querySelectorAll('.day-stop-btn').forEach(btn => {
    const dayIdx = btn.dataset.day;
    const statusEl = container.querySelector(`.day-impl-status[data-day="${dayIdx}"]`);
    bindStop(btn, statusEl);
  });
}

// ============================================================
// Generate Mode: Resilient message sending with retry
// ============================================================
async function sendMessageWithRetry(tabId, message, retries = 1) {
  for (let attempt = 0; attempt <= retries; attempt++) {
    try {
      const response = await chrome.tabs.sendMessage(tabId, message);
      return response;
    } catch (err) {
      if (attempt < retries) {
        console.warn(`[Mythos] sendMessage failed (attempt ${attempt + 1}), retrying in 2s...`, err.message);
        await new Promise(r => setTimeout(r, 2000));
        // Try pinging to check if content script is alive
        try {
          await chrome.tabs.sendMessage(tabId, { type: 'PING' });
        } catch {
          // Content script gone — try re-injecting
          try {
            await chrome.scripting.executeScript({ target: { tabId }, files: ['content.js'] });
            await new Promise(r => setTimeout(r, 500));
          } catch (injectErr) {
            console.error('[Mythos] Re-injection failed:', injectErr.message);
          }
        }
      } else {
        throw err;
      }
    }
  }
}

// ============================================================
// Generate Mode: Document verification helpers
// ============================================================
async function getDocText(tabId, retries = 0) {
  for (let attempt = 0; attempt <= retries; attempt++) {
    try {
      const response = await sendMessageWithRetry(tabId, { type: 'GET_TEXT' });
      if (response && response.success) return response.text;
    } catch {}
    if (attempt < retries) await new Promise(r => setTimeout(r, 500));
  }
  return null;
}

function normalizeForComparison(text) {
  return text
    .replace(/\r\n/g, '\n')
    .replace(/\r/g, '\n')
    .replace(/\u00A0/g, ' ')
    .replace(/[\u200B\uFEFF\u200C\u200D\u2060\uFFFE\uFFFF\u200E\u200F\u202A-\u202E\u2066-\u2069]/g, '')
    .replace(/\u2018|\u2019|\u201A|\uFF07/g, "'")
    .replace(/\u201C|\u201D|\u201E|\u201F/g, '"')
    .replace(/\u2013|\u2014|\u2012|\u2015/g, '-')
    .replace(/\u2026/g, '...')
    .replace(/\u00B7/g, '.')         // middle dot
    .replace(/\uFF0C/g, ',')        // fullwidth comma
    .replace(/\uFF0E/g, '.')        // fullwidth period
    .replace(/\u2032/g, "'")        // prime
    .replace(/\u2033/g, '"')        // double prime
    .replace(/\u00AD/g, '')         // soft hyphen
    .replace(/[ \t]+/g, ' ')
    .replace(/ ?\n ?/g, '\n')
    .replace(/\n{3,}/g, '\n\n')
    .replace(/\n+$/, '')
    .replace(/^\n+/, '')
    .trim();
}

function showMismatchWarning(statusEl, expectedText, actualText) {
  return new Promise((resolve) => {
    const wrapper = document.createElement('div');
    wrapper.className = 'mismatch-warning';

    const msg = document.createElement('div');
    if (expectedText === '' && actualText !== '') {
      msg.textContent = 'The document is not empty. Mythos will transform the current content to match this day\u2019s writing.';
    } else if (actualText === '' && expectedText !== '') {
      msg.textContent = 'The document is empty but should contain previous writing. Mythos will type this day\u2019s content from scratch.';
    } else {
      msg.textContent = 'The document content doesn\u2019t match what\u2019s expected. Mythos will transform it to match this day\u2019s writing.';
    }

    const btnRow = document.createElement('div');
    btnRow.style.cssText = 'display:flex;gap:8px;';

    const cancelBtn = document.createElement('button');
    cancelBtn.textContent = 'Cancel';
    cancelBtn.className = 'btn-outline btn-sm';
    cancelBtn.style.flex = '1';

    const proceedBtn = document.createElement('button');
    proceedBtn.textContent = 'Proceed';
    proceedBtn.className = 'btn-primary btn-sm';
    proceedBtn.style.flex = '1';

    cancelBtn.addEventListener('click', () => { wrapper.remove(); resolve(false); });
    proceedBtn.addEventListener('click', () => { wrapper.remove(); resolve(true); });

    btnRow.appendChild(cancelBtn);
    btnRow.appendChild(proceedBtn);
    wrapper.appendChild(msg);
    wrapper.appendChild(btnRow);

    if (statusEl) {
      statusEl.textContent = '';
      statusEl.className = 'status';
      statusEl.appendChild(wrapper);
      gsap.fromTo(statusEl, { opacity: 0, y: 4 }, { opacity: 1, y: 0, duration: 0.25, ease: 'power2.out' });
    } else {
      resolve(false);
    }
  });
}

// ============================================================
// Generate Mode: Implementation — diff & apply
// ============================================================
let _implementRunning = false; // Guard against concurrent calls

async function implementDay(dayIdx) {
  if (_implementRunning) {
    console.log('[Mythos] implementDay blocked — already running');
    return;
  }
  if (!currentProject) return;
  const day = currentProject.days[dayIdx];
  if (!day || day.states.length === 0) return;

  _implementRunning = true;
  typingState = 'idle';
  implementingDayIdx = null;
  console.log(`[Mythos] implementDay(${dayIdx}) starting`);

  const container = document.getElementById('days-container');
  if (!container) { _implementRunning = false; return; }

  // Helper to re-query and re-enable the button on ANY exit
  function reEnableButton() {
    const btn = container.querySelector(`.day-implement-btn[data-day="${dayIdx}"]`);
    if (btn) { btn.disabled = day.status === 'implemented'; gsap.set(btn, { opacity: 1 }); }
  }

  // All setup, verification, and typing logic is inside try/catch
  // so _implementRunning is ALWAYS reset in the finally block.
  let statusEl = null;
  let ctrlRow = null;

  try {
    const implBtn   = container.querySelector(`.day-implement-btn[data-day="${dayIdx}"]`);
    ctrlRow   = container.querySelector(`.day-ctrl-row[data-day="${dayIdx}"]`);
    const pauseBtn  = container.querySelector(`.day-pause-btn[data-day="${dayIdx}"]`);
    const stopBtn   = container.querySelector(`.day-stop-btn[data-day="${dayIdx}"]`);
    statusEl  = container.querySelector(`.day-impl-status[data-day="${dayIdx}"]`);
    const mode      = getSelectedMode(`day-${dayIdx}-mode`);

    const tab = await getActiveTab();
    if (!tab || !tab.url || !tab.url.includes('docs.google.com/document')) {
      setStatus(statusEl, 'Open a Google Doc first.', 'error');
      return;
    }

    // Same-doc detection: warn if the current doc differs from the project's doc
    const currentDocUrl = tab.url.split('?')[0];
    if (currentProject.docUrl && currentProject.docUrl !== currentDocUrl) {
      setStatus(statusEl, 'Different document detected! Open the original doc or start a New Project.', 'error');
      return;
    }
    // Update docUrl if it wasn't set yet
    if (!currentProject.docUrl) {
      currentProject.docUrl = currentDocUrl;
      await saveProject();
    }

    // Verify content script is reachable — inject if needed
    let contentScriptReady = false;
    try {
      await chrome.tabs.sendMessage(tab.id, { type: 'PING' });
      contentScriptReady = true;
    } catch {
      console.warn('[Mythos] Content script not reachable, attempting injection...');
      try {
        await chrome.scripting.executeScript({ target: { tabId: tab.id }, files: ['content.js'] });
        await new Promise(r => setTimeout(r, 1000));
        await chrome.tabs.sendMessage(tab.id, { type: 'PING' });
        contentScriptReady = true;
      } catch (retryErr) {
        console.error('[Mythos] Injection + retry failed:', retryErr);
      }
    }
    if (!contentScriptReady) {
      setStatus(statusEl, 'Content script not loaded. Reload the Google Doc, click inside it, then try again.', 'error');
      return;
    }

    try { await chrome.tabs.sendMessage(tab.id, { type: 'STOP_TYPING' }); } catch {}
    await new Promise(r => setTimeout(r, 100));

    const startIdx = day.implementedStateIndex + 1;

    // ── Determine baseline: what the doc should contain before this day's diffs ──
    let baseline = '';
    if (startIdx > 0) {
      baseline = day.states[startIdx - 1];
    } else if (dayIdx > 0) {
      const prevDay = currentProject.days[dayIdx - 1];
      baseline = prevDay.actualFinalText || (prevDay.states.length > 0 ? prevDay.states[prevDay.states.length - 1] : '');
    }

    // ── Clear document and restore baseline ──
    setStatus(statusEl, 'Preparing document...', '');
    await sendMessageWithRetry(tab.id, { type: 'CLEAR_DOC' });
    await new Promise(r => setTimeout(r, 200));

    if (baseline) {
      setStatus(statusEl, 'Restoring previous state...', '');
      await sendMessageWithRetry(tab.id, { type: 'TYPE_TEXT', text: baseline, mode: 'fast', deletions: [] });
      await new Promise(r => setTimeout(r, 200));
    }

    // ── UI setup for active implementation ──
    if (implBtn) { implBtn.disabled = true; gsap.to(implBtn, { opacity: 0.5, duration: 0.2 }); }
    if (pauseBtn) pauseBtn.disabled = false;
    if (stopBtn) stopBtn.disabled = false;
    if (ctrlRow) {
      gsap.killTweensOf(ctrlRow);
      ctrlRow.style.display = 'flex';
      gsap.set(ctrlRow, { opacity: 1, height: 'auto' });
    }
    typingState = 'typing';
    implementingDayIdx = dayIdx;

    const dayCard = document.getElementById(`day-card-${dayIdx}`);
    if (dayCard) {
      gsap.to(dayCard, { borderColor: 'var(--text)', duration: 0.3, ease: 'power2.out' });
    }

    // ── Main typing loop ──
    for (let s = startIdx; s < day.states.length; s++) {
      if (typingState === 'idle') {
        setStatus(statusEl, 'Stopped.', '');
        break;
      }

      setStatus(statusEl, `Typing state ${s + 1} of ${day.states.length}...`, '');

      const prevState = (s === startIdx) ? baseline : day.states[s - 1];
      const currState = day.states[s];
      const diffOps = computeDiff(prevState, currState);
      const cursorOps = diffToCursorOps(diffOps);

      const isPureInsert = prevState === '' && cursorOps.length === 1 && cursorOps[0].action === 'insert';

      let response;
      if (isPureInsert) {
        response = await sendMessageWithRetry(tab.id, {
          type: 'TYPE_TEXT',
          text: cursorOps[0].text,
          mode,
          deletions: [],
        });
      } else {
        response = await sendMessageWithRetry(tab.id, {
          type: 'APPLY_DIFF',
          operations: cursorOps,
          mode,
          resetCursor: true,
        });
      }

      if (typingState === 'idle') {
        setStatus(statusEl, 'Stopped.', '');
        break;
      }

      if (!response || !response.success) {
        setStatus(statusEl, 'Lost connection to editor. Click inside the doc, then try again.', 'error');
        break;
      }

      day.implementedStateIndex = s;
      await saveProject();

      if (s < day.states.length - 1) {
        let pauseMs = 0;
        if (mode === 'fast') {
          pauseMs = 0;
        } else if (mode === 'medium') {
          pauseMs = 3000 + Math.random() * 5000;
          setStatus(statusEl, `Pausing between states...`, '');
        } else if (mode === 'human') {
          pauseMs = 35000 + Math.random() * 15000;
          setStatus(statusEl, `Pausing between states...`, '');
        }
        if (pauseMs > 0) {
          const delayStart = Date.now();
          while (Date.now() - delayStart < pauseMs) {
            if (typingState === 'idle') break;
            await new Promise(r => setTimeout(r, 100));
          }
        }
      }
    }

    if (day.implementedStateIndex >= day.states.length - 1) {
      day.status = 'implemented';
      day.actualFinalText = day.states[day.states.length - 1];
      setStatus(statusEl, 'Day complete!', 'ok');
      await saveProject();
      renderDaysScreen();
    }

  } catch (err) {
    console.error('[Mythos] Implementation error:', err);
    if (typingState !== 'idle') {
      const msg = err.message || '';
      const isDisconnect = msg.includes('Receiving end does not exist') ||
                           msg.includes('Extension context invalidated') ||
                           msg.includes('Could not establish connection');
      if (isDisconnect) {
        setStatus(statusEl, 'Lost connection to Google Docs. Reload the doc tab, click inside it, and try again.', 'error');
      } else {
        setStatus(statusEl, 'Error: ' + (msg || 'Unknown error. Check console.'), 'error');
      }
    }
  } finally {
    // ALWAYS reset state — this block runs on every exit path
    console.log(`[Mythos] implementDay(${dayIdx}) exiting — cleaning up`);
    typingState = 'idle';
    implementingDayIdx = null;
    _implementRunning = false;
    // Re-query in case renderDaysScreen() recreated the DOM
    reEnableButton();
    const freshCtrlRow  = container.querySelector(`.day-ctrl-row[data-day="${dayIdx}"]`);
    const freshPauseBtn = container.querySelector(`.day-pause-btn[data-day="${dayIdx}"]`);
    const freshStopBtn  = container.querySelector(`.day-stop-btn[data-day="${dayIdx}"]`);
    if (freshPauseBtn) { freshPauseBtn.disabled = true; freshPauseBtn.textContent = 'Pause'; }
    if (freshStopBtn) freshStopBtn.disabled = true;
    if (freshCtrlRow) {
      gsap.killTweensOf(freshCtrlRow);
      gsap.to(freshCtrlRow, { opacity: 0, height: 0, duration: 0.2, ease: 'power2.in', onComplete: () => { freshCtrlRow.style.display = 'none'; } });
    }
    const freshDayCard = document.getElementById(`day-card-${dayIdx}`);
    if (freshDayCard) gsap.to(freshDayCard, { borderColor: 'var(--border)', duration: 0.3 });
  }
}

// Back to plan
document.getElementById('days-back-btn').addEventListener('click', () => {
  renderPlanScreen();
  switchScreen('plan');
});

// New project
document.getElementById('new-project-btn').addEventListener('click', async () => {
  await clearProject();
  switchScreen('input');
  document.getElementById('ai-topic').value = '';
  document.getElementById('ai-context').value = '';
  document.getElementById('days-gen-status').textContent = '';
  document.getElementById('days-container').innerHTML = '';
  document.getElementById('plan-thinking-overlay').style.display = 'none';
  document.getElementById('plan-btn-row').style.display = 'flex';
  // Reset reasoning and days to defaults
  const noneRadio = document.querySelector('input[name="ai-reasoning"][value="none"]');
  if (noneRadio) noneRadio.checked = true;
  const autoRadio = document.querySelector('input[name="ai-days"][value="auto"]');
  if (autoRadio) autoRadio.checked = true;
  // Reposition indicators
  requestAnimationFrame(() => {
    document.querySelectorAll('#ai-screen-input .mode-options').forEach(c => positionModeIndicator(c, false));
  });
});

// ============================================================
// Edit History Mode: System Prompts
// ============================================================
const DIRECTOR_SYSTEM_PROMPT = `You are a writing process director. You manage a multi-day realistic writing simulation that produces Google Docs edit history indistinguishable from a real human's writing process.

You have access to the TARGET TEXT — the final version the document should closely resemble (~95% similarity by the end). You have one tool: call_writer. This spawns a blind writer agent who produces sequential document states (snapshots of the full document at different moments). The writer CANNOT see the target text and does not know it exists. They only know what you tell them.

You operate in two phases. In Phase 1, you analyze and plan. In Phase 2 (triggered when the user confirms the plan), you execute day by day.

========================================
PHASE 1: ANALYSIS & PLANNING
========================================

Before calling any writer, perform deep analysis and produce a plan.

STEP 1 — ANALYZE THE TARGET TEXT

Read the target text carefully. Identify:
- The core thesis, argument, or central point
- Each distinct idea and the order they appear
- How ideas connect and transition
- The overall structure (how many paragraphs, how they're organized)
- Approximate word count

STEP 2 — EXTRACT STYLE & VOICE

Study HOW the text is written, not just what it says. Identify:
- Sentence structure patterns: short and punchy? Long and flowing? A deliberate mix? How do they vary?
- Vocabulary level: simple everyday words? Academic? Technical jargon? Casual slang?
- Tone: formal, conversational, passionate, detached, humorous, serious?
- Recurring habits: does the writer use dashes? Parentheticals? Rhetorical questions? Start sentences with conjunctions? Use specific filler phrases? Favor certain transitions?
- What the writer AVOIDS: no contractions? No first person? No semicolons? Never uses passive voice?
- Paragraph style: long dense paragraphs? Short punchy ones? Single-sentence paragraphs for emphasis?

Then write 2-3 example sentences that capture this exact voice. These must NOT be copied or paraphrased from the target. Write them about a completely unrelated topic (cooking, weather, sports — anything). They should sound like the same person wrote them.

STEP 3 — CREATE PERSONA

Based on the text's style, content, and sophistication, invent a specific believable writer:
- Who they are (student, professional, blogger, journalist, hobbyist)
- Why they're writing this specific piece
- Their writing habits (do they outline first? Brain dump? Procrastinate then write in a rush? Write methodically?)
- Their relationship with this topic (expert? Learning? Passionate? Doing it for an assignment?)
- Their confidence level (assured and decisive? Second-guessing? Somewhere between?)

This persona must PLAUSIBLY produce the target text.

STEP 4 — PLAN GHOST CONTENT

Ghost content is material that exists in the document temporarily across multiple days before being deleted. It makes the edit history feel alive — real writers explore ideas that don't survive.

Create 2-3 ghost content items. Each must be:
- NOT in the target text
- Topically adjacent — close enough that this specific writer would genuinely explore the idea
- Written earnestly — when the writer creates this content, they believe it belongs
- Deletable for a real reason — wrong tone, off-topic upon reflection, redundant with another point, too personal for the context

For each ghost content item, specify:
- A description of the content (not exact text — the writer will generate the actual words)
- Which day it appears
- Which day it gets cut
- The specific reason the writer cuts it, framed as their own realization

STEP 5 — PLAN DECOYS

Decoys are smaller, within-session temporary content. A sentence or phrase the writer tries and deletes within minutes. They're quicker impulses than ghost content.

For each day, plan at least 1 decoy:
- What the writer tries (a metaphor, an opening phrase, a detail, a transitional sentence)
- Why it feels right in the moment
- Why they cut it moments later (doesn't flow, too informal, redundant, leads nowhere)

STEP 6 — DAY-BY-DAY ARC

For each day, describe in 2-3 sentences:
- Session type: brain dump / restructuring / expanding / tightening / polishing
- Writer's mindset: excited but unfocused / critical with fresh eyes / frustrated / nitpicky / satisfied and wrapping up
- What broadly happens: which ideas get explored, what gets cut, what structural changes occur
- Ghost content activity: what appears or gets cut this day
- Decoy description for this day

CONVERGENCE PACING (critical):
- Day 1 of N: ~30-40% of target ideas present. Messy, rough, wrong order. Ghost content and tangents present. May contain content NOT in target.
- Middle days: ~50-70%. Structure improving. Ghost content being questioned or cut. Missing target ideas starting to appear as the writer "discovers" them.
- Second-to-last day: ~80-85%. Most target ideas present and roughly ordered. Rough edges remain. Maybe one ghost content item still alive.
- Final day: ~90%. Nearly there. Small refinements.
- Convergence (your final writer call): ~95%. Polish-level edits only.

If the user selected "auto" for number of days, recommend based on:
- Under 100 words: 2 days
- 100-300 words: 2-3 days
- 300-600 words: 3-4 days
- 600-1000 words: 4-5 days
State your recommendation.

OUTPUT YOUR PLAN in this format, then STOP and wait for user approval:

---

PERSONA:
[2-4 sentences]

STYLE & VOICE:
[Detailed description]
Example sentences:
1. "[example]"
2. "[example]"
3. "[example]"

TARGET LENGTH:
[word count range]

GHOST CONTENT:
1. [Description] | Appears: Day X | Cut: Day Y | Reason: [reason]
2. [Description] | Appears: Day X | Cut: Day Y | Reason: [reason]
3. [Description] | Appears: Day X | Cut: Day Y | Reason: [reason]

DAY-BY-DAY ARC:
Day 1: [2-3 sentence description including ghost content and decoy activity]
Day 2: [2-3 sentence description including ghost content and decoy activity]
[...continue for all days...]

RECOMMENDED DAYS: [only if user selected auto]

---

Do NOT call any tools during Phase 1. Only output the plan.

========================================
PHASE 2: EXECUTION
========================================

After the user approves the plan, execute it day by day using the call_writer tool.

For each day, follow this process:

STEP A — ASSESS CURRENT STATE

If this is Day 1, the document is empty.
If this is Day 2+, review the writer's most recent output. Assess:
- IDEAS: Which target ideas are present? Which are missing? What extra content exists?
- STRUCTURE: How close is the organization to the target?
- VOICE: Is the writing consistent with the style profile?
- CONVERGENCE: Estimate overall percentage
- PACING: Are we on track for the planned convergence at this day? Behind? Ahead?

If significantly behind: increase specificity in directions, prioritize biggest gaps.
If ahead: leave room for natural exploration, don't over-correct.

STEP B — PREPARE WRITER INSTRUCTIONS

Construct the complete message for the writer agent. MUST include: persona, full style and voice guide with example sentences, session info (day X of Y and session type), mindset framing (second person emotional state), content directions (framed as writer's inner monologue), and current document state.

CRITICAL RULES FOR WRITER INSTRUCTIONS:
1. NEVER include the target text or any direct quotes from it.
2. NEVER reference a "target", "final version", "reference text", or anything suggesting a destination exists.
3. NEVER use exact phrasing from the target in your directions.
4. Always include the full persona and style guide — the writer has no memory between calls.
5. Always include the complete current document — the writer has no memory of previous sessions.
6. Ghost content and decoy instructions must be framed as genuine creative impulses, never as temporary or disposable content.

STEP C — CALL THE WRITER

Use the call_writer tool with writer_instructions, min_states, max_states based on session type:
- Brain dump / first draft: min 7, max 12
- Restructuring: min 5, max 10
- Expanding: min 5, max 8
- Tightening: min 4, max 7
- Final polish: min 3, max 6
- Sub-session fix: min 2, max 4

STEP D — REVIEW WRITER OUTPUT

After receiving the writer's states, evaluate:
1. Did the writer stay in voice?
2. Did convergence improve as expected?
3. Are there any issues that need immediate correction?

If GOOD: move to the next day.
If SPECIFIC PROBLEM: call the writer ONCE MORE for a targeted sub-session fix (2-4 states).
Do NOT redo an entire day.

STEP E — CONVERGENCE CHECK

After the planned final day:
- If 85%+: call the writer one final time for polish (3-6 states)
- If 70-85%: one focused revision session, then polish
- If below 70%: significant revision session with more specific directions, then reassess

The FINAL state of the FINAL call should feel DONE.

After the final writer call, output:

=== COMPLETE ===

This signals the process is finished.

========================================
THROUGHOUT THE ENTIRE PROCESS:
========================================

- Before each tool call, briefly state your assessment and reasoning.
- After each tool call, briefly assess the result before proceeding.
- Maintain the consistent persona and voice across ALL writer calls.
- Keep track of ghost content lifecycle.
- The overall edit history should tell a believable story of one person writing over multiple sessions.`;

const WRITER_SYSTEM_PROMPT = `You are simulating a real human writing inside a Google Doc. You will receive information about who you are (persona), how you write (style and voice), what session this is, your current mindset, what you're working on, and the current state of the document.

You will output a sequence of STATES — snapshots of what the entire document looks like at different moments during this writing session. These states will be diffed programmatically and executed as real keystrokes in a Google Doc. The result must be indistinguishable from a real human's version history.

You are this writer. You are not simulating or performing. You are sitting at your computer, working on your document, thinking through your keyboard.

========================================
HOW YOU WRITE — INTERNALIZE THESE BEHAVIORS
========================================

FORWARD MOMENTUM:
You are writing forward. You have a sense of what you want to say but you're figuring out the specifics as you go. Each state is a natural next step from the previous one, driven by your thoughts in the moment. You do not know what the final version will look like.

UNEVEN PACING:
The amount of change between states varies dramatically.
- Sometimes you write an entire paragraph in one burst.
- Sometimes you write one sentence and pause.
- Sometimes you add just a few words to an unfinished thought.
- Sometimes you delete a large chunk.
- Sometimes you move text from one place to another.
- Sometimes you change a single word.
If every state transition is roughly the same size, it looks fake. Vary it deliberately.

TYPOS AND CORRECTIONS:
- Typos happen during fast writing bursts, not during careful editing.
- Realistic typos: transpositions (teh, adn, becuase), missing letters (writng, importnt), double letters (writting), adjacent key hits (wotk for work).
- Typos get fixed quickly — usually in the same state or the very next one.
- Most states don't have typos. Use them sparingly.

MID-SENTENCE ABANDONMENT:
Sometimes you start typing a sentence, get a few words in, realize you don't like where it's heading, and delete what you just typed to start a different sentence. This can happen mid-word.

FALSE STARTS AND WRONG TURNS:
Especially in early sessions, you may write something you genuinely think is good. A few states later, you reread it and realize it doesn't work. You cut it. When you write this content, write it earnestly.

THROAT-CLEARING:
Early drafts often start with vague, unnecessary opening sentences. These are warm-up sentences that get deleted later.

THINKING THROUGH THE KEYBOARD:
In early drafts: ideas in thinking order (not logical order), over-explanation of uncertain points, two ideas in one run-on sentence, abrupt endings.

PLACEHOLDERS AND NOTES TO SELF:
"[add example here]", "[need a better transition]", "TODO expand this"

CONFIDENCE GRADIENT:
Within a single state, some sentences come out clean and others are wordy and fumbling.

REORGANIZATION:
Real editing involves moving text. A sentence from paragraph 3 might work better as the opening.

UNDO BEHAVIOR:
Sometimes you change a word, then change it back. This is normal human indecision.

PARAGRAPH STRUCTURE:
Early drafts often come out as big blocks. Proper paragraph breaks come during restructuring.

========================================
SESSION-TYPE BEHAVIORS
========================================

BRAIN DUMP / FIRST DRAFT: Start from nothing. Write in bursts. Include throat-clearing, wandering thoughts. 7-12 states.

RESTRUCTURING: Opening doc after time away. Fresh eyes. Big moves: cutting, reordering, rewriting. 5-10 states.

EXPANDING: Identified gaps. Add new content surgically. 5-8 states.

TIGHTENING: Cut filler, trim wordy phrases, make sentences more direct. 4-7 states.

POLISHING: Small precise changes. Word swaps, comma fixes. Include undo moments. 3-6 states.

SUB-SESSION (targeted fix): Focus ONLY on one specific part. 2-4 states.

========================================
OUTPUT FORMAT
========================================

Output ONLY document states in this exact format:

=== STATE 1 ===
[Complete document text exactly as it exists at this moment]

=== STATE 2 ===
[Complete document text exactly as it exists at this moment]

[...continue for all states...]

ANTI-AI VOICE — MANDATORY:
- No em dashes. Use commas, periods, or rewrite.
- No "It's not X — it's Y" rhetorical frames.
- No stacked transitions ("Furthermore," "Moreover," "Additionally,").
- No hollow intensifiers: "truly," "deeply," "incredibly," "remarkably," "profoundly."
- No "In today's [noun]..." openings.
- No "at its core" or "at the end of the day."
- No "serves as a testament" or "navigating [abstract concept]."
- No "through the lens of" or "raises important questions about..."
Write like a real person, not like a language model.

CRITICAL RULES:
- Every state must contain the FULL document text. No shortcuts like "[rest unchanged]".
- Separate paragraphs with blank lines consistently.
- Do not include commentary or explanation outside the states. Output ONLY the states.
- The number of states must fall within the range specified in your session info.`;

// ============================================================
// Edit History Mode: Tool Definitions
// ============================================================
const CALL_WRITER_TOOL_CLAUDE = {
  name: 'call_writer',
  description: 'Spawn a blind writer agent to produce sequential document states for a writing session. The writer simulates realistic human writing behavior including typos, false starts, deletions, and mid-sentence abandonment. The writer CANNOT see the target text. Returns only the final document state.',
  input_schema: {
    type: 'object',
    properties: {
      writer_instructions: {
        type: 'string',
        description: 'Complete message for the writer: persona, style guide, session info, mindset, content directions, and current document state.',
      },
      min_states: {
        type: 'integer',
        description: 'Minimum number of document states the writer should produce.',
      },
      max_states: {
        type: 'integer',
        description: 'Maximum number of document states the writer should produce.',
      },
    },
    required: ['writer_instructions', 'min_states', 'max_states'],
  },
};

const CALL_WRITER_TOOL_GEMINI = [{
  functionDeclarations: [{
    name: 'call_writer',
    description: 'Spawn a blind writer agent to produce sequential document states for a writing session. The writer simulates realistic human writing behavior including typos, false starts, deletions, and mid-sentence abandonment. The writer CANNOT see the target text. Returns only the final document state.',
    parameters: {
      type: 'object',
      properties: {
        writer_instructions: {
          type: 'string',
          description: 'Complete message for the writer: persona, style guide, session info, mindset, content directions, and current document state.',
        },
        min_states: {
          type: 'integer',
          description: 'Minimum number of document states the writer should produce.',
        },
        max_states: {
          type: 'integer',
          description: 'Maximum number of document states the writer should produce.',
        },
      },
      required: ['writer_instructions', 'min_states', 'max_states'],
    },
  }],
}];

// ============================================================
// Edit History Mode: Non-streaming Director API calls
// ============================================================
async function callDirectorClaudeNonStreaming(apiKey, model, systemPrompt, messages, reasoning) {
  const selectedModel = model || 'claude-opus-4-6';
  const budget = CLAUDE_THINKING_BUDGETS[reasoning];
  const effectiveMaxTokens = 32000;
  const safeMaxTokens = budget ? Math.max(effectiveMaxTokens, budget + 1024) : effectiveMaxTokens;

  const body = {
    model: selectedModel,
    max_tokens: safeMaxTokens,
    system: [{ type: 'text', text: systemPrompt }],
    messages,
    tools: [CALL_WRITER_TOOL_CLAUDE],
  };
  if (budget) {
    body.thinking = { type: 'enabled', budget_tokens: budget };
  }

  const res = await fetch('https://api.anthropic.com/v1/messages', {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
      'x-api-key': apiKey,
      'anthropic-version': '2023-06-01',
      'anthropic-dangerous-direct-browser-access': 'true',
    },
    body: JSON.stringify(body),
  });

  if (!res.ok) {
    const err = await res.json().catch(() => ({}));
    throw new Error(err?.error?.message || `Claude API error ${res.status}`);
  }
  return await res.json();
}

async function callDirectorGeminiNonStreaming(apiKey, model, systemPrompt, contents, reasoning) {
  const selectedModel = model || 'gemini-3.1-pro-preview';
  const url = `https://generativelanguage.googleapis.com/v1beta/models/${selectedModel}:generateContent?key=${apiKey}`;
  const budget = GEMINI_THINKING_BUDGETS[reasoning];
  const effectiveMaxTokens = 32000;
  const safeMaxTokens = budget ? Math.min(effectiveMaxTokens + budget, 65536) : effectiveMaxTokens;

  const body = {
    contents,
    systemInstruction: { parts: [{ text: systemPrompt }] },
    tools: CALL_WRITER_TOOL_GEMINI,
    generationConfig: { maxOutputTokens: safeMaxTokens, temperature: 0.7 },
  };
  if (budget) {
    body.generationConfig.thinkingConfig = { thinkingBudget: budget, includeThoughts: true };
  }

  const res = await fetch(url, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(body),
  });

  if (!res.ok) {
    const err = await res.json().catch(() => ({}));
    throw new Error(err?.error?.message || `Gemini API error ${res.status}`);
  }
  return await res.json();
}

// Parse Director response to extract text and tool calls
function parseDirectorResponseClaude(response) {
  const textParts = [];
  const toolCalls = [];
  for (const block of (response.content || [])) {
    if (block.type === 'text') textParts.push(block.text);
    if (block.type === 'tool_use') toolCalls.push({ id: block.id, name: block.name, input: block.input });
  }
  return { text: textParts.join('\n'), toolCalls, stopReason: response.stop_reason, rawContent: response.content };
}

function parseDirectorResponseGemini(response) {
  const parts = response.candidates?.[0]?.content?.parts || [];
  const textParts = [];
  const toolCalls = [];
  for (const part of parts) {
    if (part.text && !part.thought) textParts.push(part.text);
    if (part.functionCall) toolCalls.push({ id: part.functionCall.id, name: part.functionCall.name, input: part.functionCall.args });
  }
  return { text: textParts.join('\n'), toolCalls, rawParts: parts };
}

// ============================================================
// Edit History Mode: State Management
// ============================================================
let pasteProject = null;
let editHistoryMode = false;

async function savePasteProject() {
  if (!pasteProject) return;
  await new Promise(resolve => chrome.storage.local.set({ mythosPasteProject: pasteProject }, resolve));
}

async function loadPasteProject() {
  const data = await chromeGet(['mythosPasteProject']);
  pasteProject = data.mythosPasteProject || null;
  return pasteProject;
}

async function clearPasteProject() {
  pasteProject = null;
  await new Promise(resolve => chrome.storage.local.remove('mythosPasteProject', resolve));
}

// ============================================================
// Edit History Mode: Screen Management
// ============================================================
function switchPasteScreen(screenName, animate = true) {
  const screens = document.querySelectorAll('#tab-paste .paste-screen');
  const target = document.getElementById('paste-screen-' + screenName);
  if (!target) return;

  const oldScreen = document.querySelector('#tab-paste .paste-screen.active');

  if (oldScreen && oldScreen !== target && animate) {
    gsap.to(oldScreen, {
      opacity: 0, y: 12, duration: 0.2, ease: 'power2.in',
      onComplete: () => {
        oldScreen.classList.remove('active');
        gsap.set(oldScreen, { opacity: 1, y: 0 });
        target.classList.add('active');
        gsap.set(target, { opacity: 0, y: -12 });
        gsap.to(target, { opacity: 1, y: 0, duration: 0.25, ease: 'power2.out' });
        const cards = target.querySelectorAll('.card, .day-card');
        if (cards.length) {
          gsap.fromTo(cards, { opacity: 0, y: 16 }, { opacity: 1, y: 0, duration: 0.35, stagger: 0.08, ease: 'power3.out', delay: 0.05 });
        }
        target.querySelectorAll('.mode-options').forEach(c => positionModeIndicator(c, false));
      },
    });
  } else {
    screens.forEach(s => s.classList.remove('active'));
    target.classList.add('active');
    target.querySelectorAll('.mode-options').forEach(c => positionModeIndicator(c, false));
  }

  if (pasteProject) {
    pasteProject.currentScreen = screenName;
    savePasteProject();
  }
}

// ============================================================
// Edit History Mode: Toggle
// ============================================================
document.getElementById('edit-history-btn').addEventListener('click', () => {
  editHistoryMode = !editHistoryMode;
  const btn = document.getElementById('edit-history-btn');
  const config = document.getElementById('edit-history-config');
  const speedRow = document.getElementById('paste-speed-row');
  const humanNote = document.getElementById('paste-human-note');
  const typeBtn = document.getElementById('paste-type-btn');

  if (editHistoryMode) {
    btn.classList.add('active');
    config.style.display = 'block';
    gsap.fromTo(config, { opacity: 0, height: 0 }, { opacity: 1, height: 'auto', duration: 0.3, ease: 'power2.out' });
    speedRow.style.display = 'none';
    humanNote.style.display = 'none';
    typeBtn.textContent = 'Generate';
    requestAnimationFrame(() => {
      config.querySelectorAll('.mode-options').forEach(c => positionModeIndicator(c, false));
    });
  } else {
    btn.classList.remove('active');
    gsap.to(config, { opacity: 0, height: 0, duration: 0.2, ease: 'power2.in', onComplete: () => { config.style.display = 'none'; } });
    speedRow.style.display = '';
    typeBtn.textContent = 'Start Typing';
    requestAnimationFrame(() => {
      speedRow.querySelectorAll('.mode-options').forEach(c => positionModeIndicator(c, false));
    });
  }
});

// ============================================================
// Edit History Mode: Progressive plan fill (reuses pattern from AI Generate)
// ============================================================
function progressivePastePlanFill(rawSoFar) {
  const sections = [
    { key: 'PERSONA', elId: 'paste-plan-persona' },
    { key: 'STYLE & VOICE', elId: 'paste-plan-style' },
    { key: 'TARGET LENGTH', elId: 'paste-plan-length' },
    { key: 'GHOST CONTENT', elId: 'paste-plan-ghost' },
  ];
  for (let i = 0; i < sections.length; i++) {
    const { key, elId } = sections[i];
    const regex = new RegExp(`${key.replace(/[.*+?^${}()|[\]\\]/g, '\\$&')}:\\s*\\n?`, 'i');
    const match = rawSoFar.match(regex);
    if (!match) continue;
    const startIdx = match.index + match[0].length;
    let endIdx = rawSoFar.length;
    for (let j = i + 1; j < sections.length; j++) {
      const nextRegex = new RegExp(`\\n${sections[j].key.replace(/[.*+?^${}()|[\]\\]/g, '\\$&')}:`, 'i');
      const nextMatch = rawSoFar.slice(startIdx).match(nextRegex);
      if (nextMatch) { endIdx = startIdx + nextMatch.index; break; }
    }
    const arcRegex = /\nDAY-BY-DAY ARC:/i;
    const arcMatch = rawSoFar.slice(startIdx).match(arcRegex);
    if (arcMatch && startIdx + arcMatch.index < endIdx) endIdx = startIdx + arcMatch.index;
    const recRegex = /\nRECOMMENDED DAYS:/i;
    const recMatch = rawSoFar.slice(startIdx).match(recRegex);
    if (recMatch && startIdx + recMatch.index < endIdx) endIdx = startIdx + recMatch.index;
    const text = rawSoFar.slice(startIdx, endIdx).trim();
    const el = document.getElementById(elId);
    if (text && el) { el.textContent = text; el.classList.remove('loading'); }
  }
  const arcHeaderRegex = /DAY-BY-DAY ARC:\s*\n?/i;
  const arcHeaderMatch = rawSoFar.match(arcHeaderRegex);
  if (arcHeaderMatch) {
    const arcStart = arcHeaderMatch.index + arcHeaderMatch[0].length;
    let arcEnd = rawSoFar.length;
    const recRegex2 = /\nRECOMMENDED DAYS:/i;
    const recMatch2 = rawSoFar.slice(arcStart).match(recRegex2);
    if (recMatch2) arcEnd = arcStart + recMatch2.index;
    const arcText = rawSoFar.slice(arcStart, arcEnd).trim();
    const dayRegex = /Day\s+(\d+)\s*:\s*/gi;
    let dayMatch;
    const dayPositions = [];
    while ((dayMatch = dayRegex.exec(arcText)) !== null) {
      dayPositions.push({ num: parseInt(dayMatch[1]), start: dayMatch.index + dayMatch[0].length });
    }
    if (dayPositions.length > 0) {
      const arcsList = document.getElementById('paste-plan-arcs');
      arcsList.innerHTML = '';
      for (let i = 0; i < dayPositions.length; i++) {
        const start = dayPositions[i].start;
        const end = i + 1 < dayPositions.length
          ? arcText.lastIndexOf('\n', dayPositions[i + 1].start - 1) || dayPositions[i + 1].start
          : arcText.length;
        const arcContent = arcText.slice(start, end).trim();
        const li = document.createElement('li');
        li.className = 'day-arc-item';
        li.innerHTML = `<strong>Day ${dayPositions[i].num}:</strong> ${arcContent}`;
        arcsList.appendChild(li);
      }
    }
  }
}

function renderPastePlanScreen() {
  if (!pasteProject || !pasteProject.plan) return;
  const p = pasteProject.plan;
  const ids = ['paste-plan-persona', 'paste-plan-style', 'paste-plan-length', 'paste-plan-ghost'];
  const vals = [p.persona, p.style, p.targetLength, p.ghostContent];
  ids.forEach((id, i) => { const el = document.getElementById(id); el.textContent = vals[i]; el.classList.remove('loading'); });
  const arcsList = document.getElementById('paste-plan-arcs');
  arcsList.innerHTML = '';
  p.dayArcs.forEach((arc, i) => {
    const li = document.createElement('li');
    li.className = 'day-arc-item';
    li.innerHTML = `<strong>Day ${i + 1}:</strong> ${arc}`;
    arcsList.appendChild(li);
  });
  document.getElementById('paste-plan-btn-row').style.display = 'flex';
  document.getElementById('paste-plan-btn-row').style.opacity = '1';
  document.getElementById('paste-plan-thinking-overlay').style.display = 'none';
}

// ============================================================
// Edit History Mode: Render paste days screen
// ============================================================
function renderPasteDaysScreen() {
  if (!pasteProject) return;
  const container = document.getElementById('paste-days-container');
  container.innerHTML = '';

  pasteProject.days.forEach((day, i) => {
    const card = document.createElement('div');
    card.className = 'day-card';
    card.id = `paste-day-card-${i}`;

    let badgeClass = 'badge-pending';
    let badgeText = 'Pending';
    if (day.status === 'generating') { badgeClass = 'badge-generating'; badgeText = 'Generating...'; }
    else if (day.status === 'ready') { badgeClass = 'badge-ready'; badgeText = `${day.states.length} states`; }
    else if (day.status === 'implemented') { badgeClass = 'badge-implemented'; badgeText = 'Done'; }

    const finalState = day.states.length > 0 ? day.states[day.states.length - 1] : '';
    const dayArc = pasteProject.plan?.dayArcs?.[i] || '';
    const previewText = finalState
      ? escapeHTML(finalState.length > 600 ? finalState.slice(0, 600) + '\u2026' : finalState)
      : escapeHTML(dayArc);
    const previewLabel = finalState ? 'Final draft' : 'Plan';

    card.innerHTML = `
      <div class="day-card-header" data-day="${i}">
        <div class="day-card-header-left">
          <span class="day-card-title">Day ${day.dayNumber}</span>
          <span class="status-badge ${badgeClass}">${badgeText}</span>
        </div>
        <svg class="day-card-chevron" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><polyline points="6 9 12 15 18 9"/></svg>
      </div>
      <div class="day-card-body" data-day="${i}">
        ${day.status === 'generating' ? `
          <div class="day-gen-stream" data-day="${i}">
            <div class="thinking-stream-label thinking-label-streaming">Generating</div>
            <div class="thinking-stream-window day-stream-window">
              <div class="thinking-stream-fade thinking-stream-fade-top"></div>
              <div class="day-stream-scroll" data-day="${i}"></div>
              <div class="thinking-stream-fade thinking-stream-fade-bottom"></div>
            </div>
          </div>
        ` : `
          <div class="day-card-preview-label">${previewLabel}</div>
          <div class="day-card-meta">${previewText}</div>
        `}
        ${day.status === 'ready' || day.status === 'implemented' ? `
          <div class="mode-row">
            <span class="mode-label">Speed</span>
            <div class="mode-options">
              <div class="mode-indicator"></div>
              <label><input type="radio" name="paste-day-${i}-mode" value="fast" checked><span>Fast</span></label>
              <label><input type="radio" name="paste-day-${i}-mode" value="medium"><span>Medium</span></label>
              <label><input type="radio" name="paste-day-${i}-mode" value="human"><span>Human</span></label>
            </div>
          </div>
          <button class="btn-primary paste-day-implement-btn" data-day="${i}" ${day.status === 'implemented' ? 'disabled' : ''}>
            ${day.status === 'implemented' ? 'Implemented' : `Implement Day ${day.dayNumber}`}
          </button>
          <div class="ctrl-row paste-day-ctrl-row" data-day="${i}" style="display:none;">
            <button class="btn-secondary paste-day-pause-btn" data-day="${i}" disabled>Pause</button>
            <button class="btn-secondary btn-stop paste-day-stop-btn" data-day="${i}" disabled>Stop</button>
          </div>
          <div class="status paste-day-impl-status" data-day="${i}"></div>
        ` : ''}
      </div>
    `;
    container.appendChild(card);
  });

  // Bind expand/collapse (accordion)
  container.querySelectorAll('.day-card-header').forEach(header => {
    header.addEventListener('click', () => {
      const dayIdx = header.dataset.day;
      const body = container.querySelector(`.day-card-body[data-day="${dayIdx}"]`);
      const chevron = header.querySelector('.day-card-chevron');
      const isExpanding = !body.classList.contains('expanded');
      if (!isExpanding && implementingDayIdx !== null && parseInt(dayIdx) === implementingDayIdx) return;
      if (isExpanding) {
        container.querySelectorAll('.day-card-body.expanded').forEach(ob => {
          if (ob === body) return;
          if (implementingDayIdx !== null && parseInt(ob.dataset.day) === implementingDayIdx) return;
          const oc = ob.closest('.day-card').querySelector('.day-card-chevron');
          gsap.to(ob, { height: 0, opacity: 0, duration: 0.25, ease: 'power2.in', onComplete: () => { ob.classList.remove('expanded'); ob.style.display = 'none'; ob.style.height = ''; ob.style.opacity = ''; } });
          if (oc) oc.classList.remove('expanded');
        });
      }
      if (isExpanding) {
        body.style.display = 'flex';
        body.classList.add('expanded');
        gsap.fromTo(body, { height: 0, opacity: 0 }, { height: 'auto', opacity: 1, duration: 0.3, ease: 'power2.out' });
        chevron.classList.add('expanded');
        requestAnimationFrame(() => { body.querySelectorAll('.mode-options').forEach(c => positionModeIndicator(c, false)); });
      } else {
        gsap.to(body, { height: 0, opacity: 0, duration: 0.25, ease: 'power2.in', onComplete: () => { body.classList.remove('expanded'); body.style.display = 'none'; body.style.height = ''; body.style.opacity = ''; } });
        chevron.classList.remove('expanded');
      }
    });
  });

  // Bind implement, pause, stop, mode indicators
  container.querySelectorAll('.paste-day-implement-btn').forEach(btn => {
    btn.addEventListener('click', () => implementPasteDay(parseInt(btn.dataset.day)));
  });
  container.querySelectorAll('.mode-options').forEach(mc => {
    requestAnimationFrame(() => positionModeIndicator(mc, false));
    mc.querySelectorAll('input[type="radio"]').forEach(r => { r.addEventListener('change', () => positionModeIndicator(mc, true)); });
  });
  container.querySelectorAll('.paste-day-pause-btn').forEach(btn => bindPause(btn));
  container.querySelectorAll('.paste-day-stop-btn').forEach(btn => {
    const statusEl = container.querySelector(`.paste-day-impl-status[data-day="${btn.dataset.day}"]`);
    bindStop(btn, statusEl);
  });
}

// ============================================================
// Edit History Mode: Implement a day (diff & type into doc)
// ============================================================
let _pasteImplementRunning = false;

async function implementPasteDay(dayIdx) {
  if (_pasteImplementRunning) return;
  if (!pasteProject) return;
  const day = pasteProject.days[dayIdx];
  if (!day || day.states.length === 0) return;

  _pasteImplementRunning = true;
  typingState = 'idle';
  implementingDayIdx = null;

  const container = document.getElementById('paste-days-container');
  if (!container) { _pasteImplementRunning = false; return; }

  function reEnableButton() {
    const btn = container.querySelector(`.paste-day-implement-btn[data-day="${dayIdx}"]`);
    if (btn) { btn.disabled = day.status === 'implemented'; gsap.set(btn, { opacity: 1 }); }
  }

  let statusEl = null;
  let ctrlRow = null;

  try {
    const implBtn  = container.querySelector(`.paste-day-implement-btn[data-day="${dayIdx}"]`);
    ctrlRow  = container.querySelector(`.paste-day-ctrl-row[data-day="${dayIdx}"]`);
    const pauseBtn = container.querySelector(`.paste-day-pause-btn[data-day="${dayIdx}"]`);
    const stopBtn  = container.querySelector(`.paste-day-stop-btn[data-day="${dayIdx}"]`);
    statusEl = container.querySelector(`.paste-day-impl-status[data-day="${dayIdx}"]`);
    const mode     = getSelectedMode(`paste-day-${dayIdx}-mode`);

    const tab = await getActiveTab();
    if (!tab || !tab.url || !tab.url.includes('docs.google.com/document')) {
      setStatus(statusEl, 'Open a Google Doc first.', 'error');
      return;
    }

    const currentDocUrl = tab.url.split('?')[0];
    if (pasteProject.docUrl && pasteProject.docUrl !== currentDocUrl) {
      setStatus(statusEl, 'Different document detected! Open the original doc.', 'error');
      return;
    }
    if (!pasteProject.docUrl) { pasteProject.docUrl = currentDocUrl; await savePasteProject(); }

    let contentScriptReady = false;
    try { await chrome.tabs.sendMessage(tab.id, { type: 'PING' }); contentScriptReady = true; }
    catch {
      try {
        await chrome.scripting.executeScript({ target: { tabId: tab.id }, files: ['content.js'] });
        await new Promise(r => setTimeout(r, 1000));
        await chrome.tabs.sendMessage(tab.id, { type: 'PING' });
        contentScriptReady = true;
      } catch {}
    }
    if (!contentScriptReady) { setStatus(statusEl, 'Content script not loaded. Reload the Google Doc.', 'error'); return; }

    try { await chrome.tabs.sendMessage(tab.id, { type: 'STOP_TYPING' }); } catch {}
    await new Promise(r => setTimeout(r, 100));

    const startIdx = day.implementedStateIndex + 1;

    let baseline = '';
    if (startIdx > 0) {
      baseline = day.states[startIdx - 1];
    } else if (dayIdx > 0) {
      const prevDay = pasteProject.days[dayIdx - 1];
      baseline = prevDay.actualFinalText || (prevDay.states.length > 0 ? prevDay.states[prevDay.states.length - 1] : '');
    }

    setStatus(statusEl, 'Preparing document...', '');
    await sendMessageWithRetry(tab.id, { type: 'CLEAR_DOC' });
    await new Promise(r => setTimeout(r, 200));

    if (baseline) {
      setStatus(statusEl, 'Restoring previous state...', '');
      await sendMessageWithRetry(tab.id, { type: 'TYPE_TEXT', text: baseline, mode: 'fast', deletions: [] });
      await new Promise(r => setTimeout(r, 200));
    }

    if (implBtn) { implBtn.disabled = true; gsap.to(implBtn, { opacity: 0.5, duration: 0.2 }); }
    if (pauseBtn) pauseBtn.disabled = false;
    if (stopBtn) stopBtn.disabled = false;
    if (ctrlRow) { gsap.killTweensOf(ctrlRow); ctrlRow.style.display = 'flex'; gsap.set(ctrlRow, { opacity: 1, height: 'auto' }); }
    typingState = 'typing';
    implementingDayIdx = dayIdx;

    for (let s = startIdx; s < day.states.length; s++) {
      if (typingState === 'idle') { setStatus(statusEl, 'Stopped.', ''); break; }
      setStatus(statusEl, `Typing state ${s + 1} of ${day.states.length}...`, '');

      const prevState = (s === startIdx) ? baseline : day.states[s - 1];
      const currState = day.states[s];
      const diffOps = computeDiff(prevState, currState);
      const cursorOps = diffToCursorOps(diffOps);
      const isPureInsert = prevState === '' && cursorOps.length === 1 && cursorOps[0].action === 'insert';

      let response;
      if (isPureInsert) {
        response = await sendMessageWithRetry(tab.id, { type: 'TYPE_TEXT', text: cursorOps[0].text, mode, deletions: [] });
      } else {
        response = await sendMessageWithRetry(tab.id, { type: 'APPLY_DIFF', operations: cursorOps, mode, resetCursor: true });
      }

      if (typingState === 'idle') { setStatus(statusEl, 'Stopped.', ''); break; }
      if (!response || !response.success) { setStatus(statusEl, 'Lost connection to editor.', 'error'); break; }

      day.implementedStateIndex = s;
      await savePasteProject();

      if (s < day.states.length - 1) {
        let pauseMs = 0;
        if (mode === 'medium') { pauseMs = 3000 + Math.random() * 5000; setStatus(statusEl, 'Pausing between states...', ''); }
        else if (mode === 'human') { pauseMs = 35000 + Math.random() * 15000; setStatus(statusEl, 'Pausing between states...', ''); }
        if (pauseMs > 0) {
          const delayStart = Date.now();
          while (Date.now() - delayStart < pauseMs) {
            if (typingState === 'idle') break;
            await new Promise(r => setTimeout(r, 100));
          }
        }
      }
    }

    if (day.implementedStateIndex >= day.states.length - 1) {
      day.status = 'implemented';
      day.actualFinalText = day.states[day.states.length - 1];
      setStatus(statusEl, 'Day complete!', 'ok');
      await savePasteProject();
      renderPasteDaysScreen();
    }
  } catch (err) {
    if (typingState !== 'idle') {
      const msg = err.message || '';
      const isDisconnect = msg.includes('Receiving end does not exist') || msg.includes('Extension context invalidated');
      setStatus(statusEl, isDisconnect ? 'Lost connection to Google Docs.' : 'Error: ' + msg, 'error');
    }
  } finally {
    typingState = 'idle';
    implementingDayIdx = null;
    _pasteImplementRunning = false;
    reEnableButton();
    const freshCtrlRow = container.querySelector(`.paste-day-ctrl-row[data-day="${dayIdx}"]`);
    const freshPause = container.querySelector(`.paste-day-pause-btn[data-day="${dayIdx}"]`);
    const freshStop = container.querySelector(`.paste-day-stop-btn[data-day="${dayIdx}"]`);
    if (freshPause) { freshPause.disabled = true; freshPause.textContent = 'Pause'; }
    if (freshStop) freshStop.disabled = true;
    if (freshCtrlRow) {
      gsap.killTweensOf(freshCtrlRow);
      gsap.to(freshCtrlRow, { opacity: 0, height: 0, duration: 0.2, ease: 'power2.in', onComplete: () => { freshCtrlRow.style.display = 'none'; } });
    }
  }
}

// ============================================================
// Edit History Mode: Phase 1 — Plan Generation (streaming)
// ============================================================
async function startPasteEditHistoryPlan() {
  const statusEl = document.getElementById('paste-status');
  const typeBtn  = document.getElementById('paste-type-btn');

  if (streamAbortController) {
    streamAbortController.abort();
    streamAbortController = null;
    typeBtn.textContent = 'Generate';
    setStatus(statusEl, 'Cancelled.', '');
    return;
  }

  const pasteInput = document.getElementById('paste-input');
  const { text: targetText } = parseEditableInput(pasteInput);
  if (!targetText.trim()) { setStatus(statusEl, 'Paste your target text first.', 'error'); return; }

  const provider  = document.getElementById('paste-provider').value;
  const reasoning = getSelectedMode('paste-reasoning');
  const daysVal   = getSelectedMode('paste-days');
  const stored    = await chromeGet(['claudeKey', 'claudeModel', 'geminiKey', 'geminiModel']);
  const apiKey    = provider === 'claude' ? stored.claudeKey : stored.geminiKey;
  const model     = provider === 'claude' ? stored.claudeModel : stored.geminiModel;
  if (!apiKey) { setStatus(statusEl, `No ${provider === 'claude' ? 'Claude' : 'Gemini'} API key. Go to Settings.`, 'error'); return; }

  streamAbortController = new AbortController();
  typeBtn.textContent = 'Cancel';
  setStatus(statusEl, 'Generating plan...', '');

  const hasReasoning = reasoning !== 'none';
  const numDays = daysVal === 'auto' ? null : parseInt(daysVal);
  const userMessage = `TARGET TEXT:\n"""\n${targetText}\n"""\n\nNumber of days: ${numDays || 'Auto (decide based on the text length and complexity)'}`;
  let rawPlan = '';
  let receivedText = false;
  const streamFn = provider === 'claude' ? streamClaude : streamGemini;

  const planOverlay = document.getElementById('paste-plan-thinking-overlay');
  const planThinkingLabel = document.getElementById('paste-plan-thinking-label');
  const planThinkingContent = document.getElementById('paste-plan-thinking-content');
  const planThinkingBody = document.getElementById('paste-plan-thinking-body');
  const planBtnRow = document.getElementById('paste-plan-btn-row');

  ['paste-plan-persona', 'paste-plan-style', 'paste-plan-length', 'paste-plan-ghost'].forEach(id => {
    const el = document.getElementById(id); el.textContent = ''; el.classList.add('loading');
  });
  document.getElementById('paste-plan-arcs').innerHTML = '';
  planBtnRow.style.display = 'none';

  if (hasReasoning) {
    planThinkingContent.textContent = '';
    gsap.set(planThinkingContent, { y: 0 });
    planThinkingLabel.textContent = 'Thinking';
    planThinkingLabel.classList.add('thinking-label-streaming');
    planOverlay.style.display = 'block';
    gsap.fromTo(planOverlay, { opacity: 0, y: -10 }, { opacity: 1, y: 0, duration: 0.3, ease: 'power2.out' });
  } else {
    planOverlay.style.display = 'none';
  }

  switchPasteScreen('plan');

  try {
    await streamFn({
      apiKey, model,
      prompt: userMessage,
      system: DIRECTOR_SYSTEM_PROMPT,
      reasoning,
      maxTokens: 16000,
      onThinking: (chunk) => {
        if (hasReasoning) {
          planThinkingContent.textContent += chunk;
          const overflow = planThinkingContent.scrollHeight - planThinkingBody.clientHeight;
          if (overflow > 0) gsap.to(planThinkingContent, { y: -overflow, duration: 0.3, ease: 'power1.out', overwrite: true });
        }
      },
      onText: (chunk) => {
        if (!receivedText && hasReasoning) {
          receivedText = true;
          planThinkingLabel.textContent = 'Thinking \u2014 done';
          planThinkingLabel.classList.remove('thinking-label-streaming');
          gsap.to(planOverlay, { opacity: 0, y: -10, duration: 0.5, delay: 0.8, ease: 'power2.in', onComplete: () => { planOverlay.style.display = 'none'; } });
        }
        rawPlan += chunk;
        progressivePastePlanFill(rawPlan);
      },
      signal: streamAbortController.signal,
    });

    if (hasReasoning && !receivedText) {
      planThinkingLabel.textContent = 'Thinking \u2014 done';
      planThinkingLabel.classList.remove('thinking-label-streaming');
      gsap.to(planOverlay, { opacity: 0, y: -10, duration: 0.5, ease: 'power2.in', onComplete: () => { planOverlay.style.display = 'none'; } });
    }

    const plan = parsePlanOutput(rawPlan);
    if (!plan.persona && !plan.style) {
      setStatus(statusEl, 'Plan format not recognized. Try regenerating.', 'error');
      streamAbortController = null;
      typeBtn.textContent = 'Generate';
      switchPasteScreen('input');
      return;
    }

    renderPastePlanScreen();
    ['paste-plan-persona', 'paste-plan-style', 'paste-plan-length', 'paste-plan-ghost'].forEach(id => {
      document.getElementById(id).classList.remove('loading');
    });
    planBtnRow.style.display = 'flex';
    gsap.fromTo(planBtnRow, { opacity: 0, y: 10 }, { opacity: 1, y: 0, duration: 0.3, ease: 'power2.out' });

    const finalDays = numDays || plan.dayArcs.length || 3;
    let docUrl = '';
    try { const tab = await getActiveTab(); if (tab?.url?.includes('docs.google.com/document')) docUrl = tab.url.split('?')[0]; } catch {}

    pasteProject = {
      id: String(Date.now()),
      targetText,
      provider,
      reasoning,
      numDays: finalDays,
      docUrl,
      plan,
      rawPlan,
      days: Array.from({ length: finalDays }, (_, i) => ({
        dayNumber: i + 1,
        status: 'pending',
        states: [],
        implementedStateIndex: -1,
      })),
      currentScreen: 'plan',
      directorConversation: null,
    };
    await savePasteProject();
    setStatus(statusEl, '', '');

  } catch (err) {
    if (err.name === 'AbortError') {
      setStatus(statusEl, 'Cancelled.', '');
    } else {
      setStatus(statusEl, 'Error: ' + err.message, 'error');
    }
    switchPasteScreen('input');
    planOverlay.style.display = 'none';
    planThinkingLabel.classList.remove('thinking-label-streaming');
  } finally {
    streamAbortController = null;
    typeBtn.textContent = editHistoryMode ? 'Generate' : 'Start Typing';
  }
}

// ============================================================
// Edit History Mode: Phase 2 — Director execution loop
// ============================================================
async function executePasteEditHistory() {
  if (!pasteProject) return;

  const statusEl    = document.getElementById('paste-days-gen-status');
  const progressEl  = document.getElementById('paste-days-progress');
  const progressBar = document.getElementById('paste-days-progress-bar');
  progressEl.style.display = 'block';

  const stored   = await chromeGet(['claudeKey', 'claudeModel', 'geminiKey', 'geminiModel']);
  const provider = pasteProject.provider;
  const apiKey   = provider === 'claude' ? stored.claudeKey : stored.geminiKey;
  const model    = provider === 'claude' ? stored.claudeModel : stored.geminiModel;
  const reasoning = pasteProject.reasoning || 'none';
  const streamFn = provider === 'claude' ? streamClaude : streamGemini;

  // Build initial Director conversation
  const targetUserMsg = `TARGET TEXT:\n"""\n${pasteProject.targetText}\n"""\n\nNumber of days: ${pasteProject.numDays}`;

  let claudeMessages, geminiContents;
  if (provider === 'claude') {
    claudeMessages = [
      { role: 'user', content: targetUserMsg },
      { role: 'assistant', content: [{ type: 'text', text: pasteProject.rawPlan }] },
      { role: 'user', content: 'Plan approved. Begin execution.' },
    ];
  } else {
    geminiContents = [
      { role: 'user', parts: [{ text: targetUserMsg }] },
      { role: 'model', parts: [{ text: pasteProject.rawPlan }] },
      { role: 'user', parts: [{ text: 'Plan approved. Begin execution.' }] },
    ];
  }

  let allDaysComplete = false;
  let iterationCount = 0;
  const maxIterations = pasteProject.numDays * 4 + 10;

  while (!allDaysComplete && iterationCount < maxIterations) {
    iterationCount++;
    const completedDays = pasteProject.days.filter(d => d.status === 'ready' || d.status === 'implemented').length;
    progressBar.style.width = `${(completedDays / pasteProject.numDays) * 100}%`;
    setWaveStatus(statusEl, `Director is working (round ${iterationCount})...`);

    let directorResult;
    try {
      if (provider === 'claude') {
        const rawResp = await callDirectorClaudeNonStreaming(apiKey, model, DIRECTOR_SYSTEM_PROMPT, claudeMessages, reasoning);
        directorResult = parseDirectorResponseClaude(rawResp);
        claudeMessages.push({ role: 'assistant', content: rawResp.content });
      } else {
        const rawResp = await callDirectorGeminiNonStreaming(apiKey, model, DIRECTOR_SYSTEM_PROMPT, geminiContents, reasoning);
        directorResult = parseDirectorResponseGemini(rawResp);
        geminiContents.push({ role: 'model', parts: rawResp.candidates[0].content.parts });
      }
    } catch (err) {
      setStatus(statusEl, 'Director error: ' + err.message, 'error');
      progressEl.style.display = 'none';
      return;
    }

    // Check for completion
    if (directorResult.text.includes('=== COMPLETE ===')) {
      allDaysComplete = true;
      break;
    }

    // If no tool call, nudge the Director
    if (directorResult.toolCalls.length === 0) {
      if (provider === 'claude') {
        claudeMessages.push({ role: 'user', content: 'Continue. Call the writer for the next session.' });
      } else {
        geminiContents.push({ role: 'user', parts: [{ text: 'Continue. Call the writer for the next session.' }] });
      }
      continue;
    }

    // Process each tool call
    for (const tc of directorResult.toolCalls) {
      if (tc.name !== 'call_writer') continue;

      const { writer_instructions, min_states, max_states } = tc.input;
      const stateRange = `\n\nProduce between ${min_states || 5} and ${max_states || 10} states for this session.`;
      const writerPrompt = writer_instructions + stateRange;

      // Find the next pending day to assign these states to
      const nextPendingIdx = pasteProject.days.findIndex(d => d.status === 'pending');
      if (nextPendingIdx >= 0) {
        pasteProject.days[nextPendingIdx].status = 'generating';
        renderPasteDaysScreen();
        setWaveStatus(statusEl, `Generating Day ${nextPendingIdx + 1}...`);

        requestAnimationFrame(() => {
          const genHeader = document.querySelector(`.day-card-header[data-day="${nextPendingIdx}"]`);
          const genBody = document.querySelector(`.day-card-body[data-day="${nextPendingIdx}"]`);
          if (genHeader && genBody && !genBody.classList.contains('expanded')) genHeader.click();
        });
      }

      // Call the Writer (streaming)
      let rawWriter = '';
      let writerSuccess = false;
      for (let attempt = 0; attempt < 2; attempt++) {
        rawWriter = '';
        try {
          await streamFn({
            apiKey, model,
            prompt: writerPrompt,
            system: WRITER_SYSTEM_PROMPT,
            reasoning,
            maxTokens: 48000,
            onThinking: () => {},
            onText: (chunk) => {
              rawWriter += chunk;
              if (nextPendingIdx >= 0) {
                const streamEl = document.querySelector(`.day-stream-scroll[data-day="${nextPendingIdx}"]`);
                if (streamEl) {
                  streamEl.textContent = rawWriter;
                  const windowEl = streamEl.parentElement;
                  if (windowEl) {
                    const overflow = streamEl.scrollHeight - windowEl.clientHeight;
                    if (overflow > 0) gsap.to(streamEl, { y: -overflow, duration: 0.3, ease: 'power1.out', overwrite: true });
                  }
                }
              }
            },
            signal: null,
          });
          const states = parseDayOutput(rawWriter);
          if (states.length > 0) {
            writerSuccess = true;
            if (nextPendingIdx >= 0) {
              pasteProject.days[nextPendingIdx].states = states;
              pasteProject.days[nextPendingIdx].status = 'ready';
              await savePasteProject();
              renderPasteDaysScreen();
            }
            // Return final state to Director
            const finalState = states[states.length - 1];
            const toolResult = `Writer session complete. ${states.length} states produced.\n\nFinal document state:\n"""\n${finalState}\n"""`;
            if (provider === 'claude') {
              claudeMessages.push({ role: 'user', content: [{ type: 'tool_result', tool_use_id: tc.id, content: toolResult }] });
            } else {
              geminiContents.push({ role: 'user', parts: [{ functionResponse: { name: 'call_writer', id: tc.id, response: { content: toolResult } } }] });
            }
            break;
          }
        } catch (err) {
          console.error('[Mythos] Writer call failed:', err);
        }
        if (attempt === 0) {
          setStatus(statusEl, 'Writer produced no states, retrying...', '');
          await new Promise(r => setTimeout(r, 2000));
        }
      }

      if (!writerSuccess) {
        if (nextPendingIdx >= 0) { pasteProject.days[nextPendingIdx].status = 'pending'; renderPasteDaysScreen(); }
        const errorResult = 'Writer session failed to produce valid states. Please provide revised instructions and try again.';
        if (provider === 'claude') {
          claudeMessages.push({ role: 'user', content: [{ type: 'tool_result', tool_use_id: tc.id, content: errorResult, is_error: true }] });
        } else {
          geminiContents.push({ role: 'user', parts: [{ functionResponse: { name: 'call_writer', id: tc.id, response: { error: errorResult } } }] });
        }
      }

      await new Promise(r => setTimeout(r, 2000));
    }
  }

  progressBar.style.width = '100%';
  progressEl.style.display = 'none';

  const readyCount = pasteProject.days.filter(d => d.status === 'ready' || d.status === 'implemented').length;
  if (readyCount > 0) {
    setStatus(statusEl, `Generation complete! ${readyCount} day(s) ready to implement.`, 'ok');
    setTimeout(() => {
      if (statusEl.textContent.includes('complete')) {
        gsap.to(statusEl, { opacity: 0, y: -4, duration: 0.5, ease: 'power2.in', onComplete: () => { statusEl.textContent = ''; statusEl.style.opacity = ''; statusEl.style.transform = ''; } });
      }
    }, 3000);
  } else {
    setStatus(statusEl, 'Generation finished but no days were produced. Try again.', 'error');
  }

  // Auto-expand first actionable day
  const firstReady = pasteProject.days.findIndex(d => d.status === 'ready');
  if (firstReady >= 0) {
    requestAnimationFrame(() => {
      const header = document.querySelector(`#paste-days-container .day-card-header[data-day="${firstReady}"]`);
      if (header) header.click();
    });
  }
}

// ============================================================
// Edit History Mode: Plan screen event wiring
// ============================================================
// Back to input
document.getElementById('paste-plan-back-btn').addEventListener('click', () => {
  if (streamAbortController) { streamAbortController.abort(); streamAbortController = null; }
  document.getElementById('paste-plan-thinking-overlay').style.display = 'none';
  document.getElementById('paste-type-btn').textContent = editHistoryMode ? 'Generate' : 'Start Typing';
  switchPasteScreen('input');
});

// Edit plan
document.getElementById('paste-plan-edit-btn').addEventListener('click', () => {
  document.getElementById('paste-plan-edit-row').style.display = 'block';
  document.getElementById('paste-plan-btn-row').style.display = 'none';
  document.getElementById('paste-plan-edit-input').value = '';
  document.getElementById('paste-plan-edit-input').focus();
});

document.getElementById('paste-plan-edit-cancel').addEventListener('click', () => {
  document.getElementById('paste-plan-edit-row').style.display = 'none';
  document.getElementById('paste-plan-btn-row').style.display = 'flex';
});

// Apply plan edit
document.getElementById('paste-plan-edit-submit').addEventListener('click', async () => {
  const instructions = document.getElementById('paste-plan-edit-input').value.trim();
  if (!instructions) return;
  const statusEl = document.getElementById('paste-plan-status');
  if (!pasteProject || !pasteProject.plan) return;

  const stored = await chromeGet(['claudeKey', 'claudeModel', 'geminiKey', 'geminiModel']);
  const provider = pasteProject.provider;
  const apiKey = provider === 'claude' ? stored.claudeKey : stored.geminiKey;
  const model  = provider === 'claude' ? stored.claudeModel : stored.geminiModel;
  const streamFnLocal = provider === 'claude' ? streamClaude : streamGemini;
  const reasoning = pasteProject.reasoning || 'none';
  const hasReasoning = reasoning !== 'none';

  if (!apiKey) { setStatus(statusEl, 'No API key. Go to Settings.', 'error'); return; }

  document.getElementById('paste-plan-edit-row').style.display = 'none';
  ['paste-plan-persona', 'paste-plan-style', 'paste-plan-length', 'paste-plan-ghost'].forEach(id => {
    document.getElementById(id).classList.add('loading');
  });
  const planBtnRow = document.getElementById('paste-plan-btn-row');
  planBtnRow.style.display = 'none';

  const planOverlay = document.getElementById('paste-plan-thinking-overlay');
  const planThinkingLabel = document.getElementById('paste-plan-thinking-label');
  const planThinkingContent = document.getElementById('paste-plan-thinking-content');
  const planThinkingBody = document.getElementById('paste-plan-thinking-body');

  if (hasReasoning) {
    planThinkingContent.textContent = '';
    gsap.set(planThinkingContent, { y: 0 });
    planThinkingLabel.textContent = 'Thinking';
    planThinkingLabel.classList.add('thinking-label-streaming');
    planOverlay.style.display = 'block';
    gsap.fromTo(planOverlay, { opacity: 0, y: -10 }, { opacity: 1, y: 0, duration: 0.3, ease: 'power2.out' });
  }

  streamAbortController = new AbortController();
  setStatus(statusEl, 'Revising plan...', '');

  const editSystem = DIRECTOR_SYSTEM_PROMPT + '\n\n---\n\nYou are REVISING an existing plan. Output the COMPLETE revised plan in the exact same format. Do not output explanations.';
  const editUser = `CURRENT PLAN:\n${pasteProject.rawPlan}\n\n---\n\nREQUESTED CHANGES:\n${instructions}\n\nRevise the plan. Output the full revised plan.`;

  let rawPlan = '';
  let receivedText = false;

  try {
    await streamFnLocal({
      apiKey, model, prompt: editUser, system: editSystem, reasoning, maxTokens: 16000,
      onThinking: (chunk) => {
        if (hasReasoning) {
          planThinkingContent.textContent += chunk;
          const overflow = planThinkingContent.scrollHeight - planThinkingBody.clientHeight;
          if (overflow > 0) gsap.to(planThinkingContent, { y: -overflow, duration: 0.3, ease: 'power1.out', overwrite: true });
        }
      },
      onText: (chunk) => {
        if (!receivedText && hasReasoning) {
          receivedText = true;
          planThinkingLabel.textContent = 'Thinking \u2014 done';
          planThinkingLabel.classList.remove('thinking-label-streaming');
          gsap.to(planOverlay, { opacity: 0, y: -10, duration: 0.5, delay: 0.8, ease: 'power2.in', onComplete: () => { planOverlay.style.display = 'none'; } });
        }
        rawPlan += chunk;
        progressivePastePlanFill(rawPlan);
      },
      signal: streamAbortController.signal,
    });

    if (hasReasoning && !receivedText) {
      planThinkingLabel.textContent = 'Thinking \u2014 done';
      planThinkingLabel.classList.remove('thinking-label-streaming');
      gsap.to(planOverlay, { opacity: 0, y: -10, duration: 0.5, ease: 'power2.in', onComplete: () => { planOverlay.style.display = 'none'; } });
    }

    const plan = parsePlanOutput(rawPlan);
    if (!plan.persona && !plan.style) { setStatus(statusEl, 'Revised plan not recognized. Try again.', 'error'); renderPastePlanScreen(); planBtnRow.style.display = 'flex'; streamAbortController = null; return; }

    pasteProject.plan = plan;
    pasteProject.rawPlan = rawPlan;
    const finalDays = pasteProject.numDays || plan.dayArcs.length || 3;
    pasteProject.numDays = finalDays;
    pasteProject.days = Array.from({ length: finalDays }, (_, i) => ({ dayNumber: i + 1, status: 'pending', states: [], implementedStateIndex: -1 }));
    await savePasteProject();

    renderPastePlanScreen();
    ['paste-plan-persona', 'paste-plan-style', 'paste-plan-length', 'paste-plan-ghost'].forEach(id => document.getElementById(id).classList.remove('loading'));
    planBtnRow.style.display = 'flex';
    gsap.fromTo(planBtnRow, { opacity: 0, y: 10 }, { opacity: 1, y: 0, duration: 0.3, ease: 'power2.out' });
    setStatus(statusEl, 'Plan updated!', 'ok');
  } catch (err) {
    if (err.name === 'AbortError') setStatus(statusEl, 'Edit cancelled.', '');
    else setStatus(statusEl, 'Error: ' + (err.message || 'Unknown'), 'error');
    renderPastePlanScreen();
    planBtnRow.style.display = 'flex';
    planOverlay.style.display = 'none';
  } finally {
    streamAbortController = null;
  }
});

// Approve & Generate
document.getElementById('paste-plan-approve-btn').addEventListener('click', async () => {
  switchPasteScreen('days');
  renderPasteDaysScreen();
  await executePasteEditHistory();
});

// Days screen: back to plan
document.getElementById('paste-days-back-btn').addEventListener('click', () => {
  renderPastePlanScreen();
  switchPasteScreen('plan');
});

// Days screen: new project
document.getElementById('paste-new-project-btn').addEventListener('click', async () => {
  await clearPasteProject();
  switchPasteScreen('input');
  document.getElementById('paste-days-gen-status').textContent = '';
  document.getElementById('paste-days-container').innerHTML = '';
  document.getElementById('paste-plan-thinking-overlay').style.display = 'none';
  document.getElementById('paste-plan-btn-row').style.display = 'flex';
});

// ============================================================
// Edit History Mode: Restore saved paste project on open
// ============================================================
(async () => {
  const proj = await loadPasteProject();
  if (proj) {
    editHistoryMode = true;
    const btn = document.getElementById('edit-history-btn');
    const config = document.getElementById('edit-history-config');
    const speedRow = document.getElementById('paste-speed-row');
    const typeBtn = document.getElementById('paste-type-btn');
    btn.classList.add('active');
    config.style.display = 'block';
    speedRow.style.display = 'none';
    typeBtn.textContent = 'Generate';

    if (proj.provider) document.getElementById('paste-provider').value = proj.provider;
    if (proj.reasoning) {
      const rr = document.querySelector(`input[name="paste-reasoning"][value="${proj.reasoning}"]`);
      if (rr) rr.checked = true;
    }
    if (proj.numDays) {
      const dv = proj.numDays <= 7 ? String(proj.numDays) : 'auto';
      const dr = document.querySelector(`input[name="paste-days"][value="${dv}"]`);
      if (dr) dr.checked = true;
    }

    if (proj.currentScreen === 'plan' && proj.plan) {
      renderPastePlanScreen();
      switchPasteScreen('plan', false);
    } else if (proj.currentScreen === 'days' && proj.days) {
      proj.days.forEach(d => { if (d.status === 'generating') d.status = 'pending'; });
      await savePasteProject();
      renderPasteDaysScreen();
      switchPasteScreen('days', false);
      const nextDayIdx = proj.days.findIndex(d => d.status === 'ready');
      if (nextDayIdx >= 0) {
        requestAnimationFrame(() => {
          const header = document.querySelector(`#paste-days-container .day-card-header[data-day="${nextDayIdx}"]`);
          if (header) header.click();
        });
      }
    }

    requestAnimationFrame(() => {
      config.querySelectorAll('.mode-options').forEach(c => positionModeIndicator(c, false));
    });
  }
})();

// ============================================================
// Cross Out button — wraps selection in <del> or unwraps it
// ============================================================
document.getElementById('crossout-btn').addEventListener('click', () => {
  const input = document.getElementById('paste-input');
  const selection = window.getSelection();
  if (!selection.rangeCount || selection.isCollapsed) return;

  const range = selection.getRangeAt(0);
  // Ensure selection is within our contenteditable
  if (!input.contains(range.commonAncestorContainer)) return;

  // Check if entire selection is already inside a single <del>
  const parentDel = range.commonAncestorContainer.nodeType === Node.TEXT_NODE
    ? range.commonAncestorContainer.parentElement.closest('del')
    : range.commonAncestorContainer.closest?.('del');

  if (parentDel && input.contains(parentDel)) {
    // Unwrap: replace <del> with its text content
    const text = document.createTextNode(parentDel.textContent);
    parentDel.replaceWith(text);
    // Re-select the unwrapped text
    const newRange = document.createRange();
    newRange.selectNodeContents(text);
    selection.removeAllRanges();
    selection.addRange(newRange);
  } else {
    // Wrap selection in <del>
    const del = document.createElement('del');
    try {
      range.surroundContents(del);
    } catch {
      // surroundContents fails if selection crosses element boundaries
      // Fallback: extract, wrap, and re-insert
      const fragment = range.extractContents();
      del.appendChild(fragment);
      range.insertNode(del);
    }
    // Select the new <del> contents
    const newRange = document.createRange();
    newRange.selectNodeContents(del);
    selection.removeAllRanges();
    selection.addRange(newRange);
  }

  // Normalize to merge adjacent text nodes
  input.normalize();
});

// ============================================================
// Parse contenteditable: extract plain text + deletion ranges
// ============================================================
function parseEditableInput(container) {
  let text = '';
  const deletions = [];

  function walk(node, insideDel) {
    if (node.nodeType === Node.TEXT_NODE) {
      const content = node.textContent;
      if (insideDel && content.length > 0) {
        deletions.push({ start: text.length, end: text.length + content.length });
      }
      text += content;
    } else if (node.nodeType === Node.ELEMENT_NODE) {
      if (node.tagName === 'BR') {
        text += '\n';
        return;
      }
      const isDel = insideDel || node.tagName === 'DEL';
      for (const child of node.childNodes) {
        walk(child, isDel);
      }
      // Block elements (div, p) add a newline after, unless it's the last child
      const blockTags = ['DIV', 'P'];
      if (blockTags.includes(node.tagName) && node.nextSibling) {
        text += '\n';
      }
    }
  }

  for (const child of container.childNodes) {
    walk(child, false);
  }

  return { text, deletions };
}

// ============================================================
// Typing state machine
// Shared between Paste and AI panels
// ============================================================
let typingState = 'idle'; // 'idle' | 'typing' | 'paused'
let implementingDayIdx = null; // Track which day is currently being implemented

function getSelectedMode(radioName) {
  return document.querySelector(`input[name="${radioName}"]:checked`).value;
}

async function getActiveTab() {
  const [tab] = await chrome.tabs.query({ active: true, currentWindow: true });
  return tab;
}

// Start typing — used by both panels
// getData returns { text, deletions } or just a string (AI panel)
async function startTyping({ getData, radioName, startBtn, pauseBtn, stopBtn, ctrlRow, statusEl }) {
  const result = getData();
  const text = typeof result === 'string' ? result : result.text;
  const deletions = typeof result === 'string' ? [] : (result.deletions || []);

  if (!text) {
    setStatus(statusEl, 'Nothing to type.', 'error');
    return;
  }

  const tab = await getActiveTab();
  if (!tab || !tab.url || !tab.url.includes('docs.google.com/document')) {
    setStatus(statusEl, 'Open a Google Doc first.', 'error');
    return;
  }

  const mode = getSelectedMode(radioName);
  typingState = 'typing';
  startBtn.disabled = true;
  pauseBtn.disabled = false;
  stopBtn.disabled  = false;

  // Show ctrl row with animation
  if (ctrlRow) {
    ctrlRow.style.display = 'flex';
    gsap.fromTo(ctrlRow, { opacity: 0, height: 0 }, { opacity: 1, height: 'auto', duration: 0.25, ease: 'power2.out' });
  }

  const hasDeletions = deletions.length > 0;
  if (hasDeletions) {
    setStatus(statusEl, mode === 'human' ? 'Typing (human mode)... will delete crossed-out text after.' : 'Typing... will delete crossed-out text after.', '');
  } else {
    setStatus(statusEl, mode === 'human' ? 'Typing (human mode)...' : 'Typing...', '');
  }

  try {
    const response = await sendMessageWithRetry(tab.id, {
      type: 'TYPE_TEXT',
      text,
      mode,
      deletions,
    });

    if (typingState === 'idle') {
      setStatus(statusEl, 'Stopped.', '');
    } else if (response && response.success) {
      setStatus(statusEl, 'Done!', 'ok');
    } else {
      setStatus(statusEl, 'Could not find editor. Click inside the doc first.', 'error');
    }
  } catch {
    if (typingState !== 'idle') {
      setStatus(statusEl, 'Error. Reload the Google Doc and try again.', 'error');
    }
  } finally {
    typingState = 'idle';
    if (startBtn) startBtn.disabled = false;
    if (pauseBtn) { pauseBtn.disabled = true; pauseBtn.textContent = 'Pause'; }
    if (stopBtn) stopBtn.disabled = true;
    // Hide ctrl row
    if (ctrlRow) {
      gsap.to(ctrlRow, { opacity: 0, height: 0, duration: 0.2, ease: 'power2.in', onComplete: () => { ctrlRow.style.display = 'none'; } });
    }
  }
}

// Pause / Resume
function bindPause(pauseBtn) {
  pauseBtn.addEventListener('click', async () => {
    const tab = await getActiveTab();
    if (!tab) return;

    if (typingState === 'typing') {
      typingState = 'paused';
      pauseBtn.textContent = 'Resume';
      chrome.tabs.sendMessage(tab.id, { type: 'PAUSE_TYPING' });
    } else if (typingState === 'paused') {
      typingState = 'typing';
      pauseBtn.textContent = 'Pause';
      chrome.tabs.sendMessage(tab.id, { type: 'RESUME_TYPING' });
    }
  });
}

// Stop
function bindStop(stopBtn, statusEl) {
  stopBtn.addEventListener('click', async () => {
    const tab = await getActiveTab();
    if (!tab) return;
    typingState = 'idle';
    chrome.tabs.sendMessage(tab.id, { type: 'STOP_TYPING' });
    setStatus(statusEl, 'Stopped.', '');
  });
}

// ============================================================
// Paste panel wiring
// ============================================================
const pasteStartBtn = document.getElementById('paste-type-btn');
const pastePauseBtn = document.getElementById('paste-pause-btn');
const pasteStopBtn  = document.getElementById('paste-stop-btn');
const pasteCtrlRow  = document.getElementById('paste-ctrl-row');
const pasteStatus   = document.getElementById('paste-status');

pasteStartBtn.addEventListener('click', () => {
  if (editHistoryMode) {
    startPasteEditHistoryPlan();
  } else {
    startTyping({
      getData:   () => parseEditableInput(document.getElementById('paste-input')),
      radioName: 'paste-mode',
      startBtn:  pasteStartBtn,
      pauseBtn:  pastePauseBtn,
      stopBtn:   pasteStopBtn,
      ctrlRow:   pasteCtrlRow,
      statusEl:  pasteStatus,
    });
  }
});
bindPause(pastePauseBtn);
bindStop(pasteStopBtn, pasteStatus);

// (Old AI panel wiring removed — replaced by Generate Mode)

// ============================================================
// Helpers
// ============================================================
function setStatus(el, msg, type) {
  if (!el) return;
  if (el.textContent) {
    gsap.to(el, {
      opacity: 0, y: -4, duration: 0.12, ease: 'power2.in',
      onComplete: () => {
        el.textContent = msg;
        el.className = 'status' + (type ? ' ' + type : '');
        gsap.fromTo(el, { opacity: 0, y: 4 }, { opacity: 1, y: 0, duration: 0.2, ease: 'power2.out' });
      },
    });
  } else {
    el.textContent = msg;
    el.className = 'status' + (type ? ' ' + type : '');
    gsap.fromTo(el, { opacity: 0, y: 4 }, { opacity: 1, y: 0, duration: 0.2, ease: 'power2.out' });
  }
}

function chromeGet(keys) {
  return new Promise((resolve) => chrome.storage.local.get(keys, resolve));
}

// Wave-animated status text (each character animates with staggered delay)
function setWaveStatus(el, msg) {
  el.className = 'status';
  el.innerHTML = '';
  const wrapper = document.createElement('span');
  wrapper.className = 'wave-text';
  for (let i = 0; i < msg.length; i++) {
    const span = document.createElement('span');
    span.textContent = msg[i] === ' ' ? '\u00A0' : msg[i];
    span.style.animationDelay = `${i * 0.04}s`;
    wrapper.appendChild(span);
  }
  el.appendChild(wrapper);
  gsap.fromTo(el, { opacity: 0 }, { opacity: 1, duration: 0.3, ease: 'power2.out' });
}

// ============================================================
// GSAP: Page load entrance animation
// ============================================================
document.addEventListener('DOMContentLoaded', () => {
  const tl = gsap.timeline({ defaults: { ease: 'power2.out' } });

  // Header logo entrance
  tl.fromTo('.header img',
    { opacity: 0, y: -12 },
    { opacity: 1, y: 0, duration: 0.45 }
  );

  // Tabs stagger
  tl.fromTo('.tab-btn',
    { opacity: 0, y: 10 },
    { opacity: 1, y: 0, duration: 0.35, stagger: 0.08 },
    '-=0.2'
  );

  // Tab indicator fade in
  tl.fromTo('.tab-indicator',
    { opacity: 0 },
    { opacity: 1, duration: 0.3 },
    '-=0.1'
  );

  // Active panel cards stagger
  const activeCards = document.querySelectorAll('.panel.active .card');
  if (activeCards.length) {
    tl.fromTo(activeCards,
      { opacity: 0, y: 20 },
      { opacity: 1, y: 0, duration: 0.45, stagger: 0.12, ease: 'power3.out' },
      '-=0.1'
    );
  }
});

// ============================================================
// GSAP: Button hover micro-interactions (event delegation for dynamic buttons)
// ============================================================
document.body.addEventListener('mouseenter', (e) => {
  const btn = e.target.closest('.btn-primary, .btn-outline, .btn-new-project');
  if (btn && !btn.disabled) {
    gsap.to(btn, { scale: 1.02, y: -1, duration: 0.2, ease: 'power2.out' });
  }
  const saveBtn = e.target.closest('.btn-save');
  if (saveBtn) {
    gsap.to(saveBtn, { scale: 1.05, duration: 0.2, ease: 'power2.out' });
  }
  const backBtn = e.target.closest('.screen-back');
  if (backBtn) {
    gsap.to(backBtn, { x: -3, duration: 0.2, ease: 'power2.out' });
  }
  const dayHeader = e.target.closest('.day-card-header');
  if (dayHeader) {
    const card = dayHeader.closest('.day-card');
    if (card) gsap.to(card, { boxShadow: '0 4px 16px rgba(0,0,0,0.08)', duration: 0.2 });
  }
}, true);

document.body.addEventListener('mouseleave', (e) => {
  const btn = e.target.closest('.btn-primary, .btn-outline, .btn-new-project');
  if (btn) {
    gsap.to(btn, { scale: 1, y: 0, duration: 0.25, ease: 'power2.inOut' });
  }
  const saveBtn = e.target.closest('.btn-save');
  if (saveBtn) {
    gsap.to(saveBtn, { scale: 1, duration: 0.2, ease: 'power2.inOut' });
  }
  const backBtn = e.target.closest('.screen-back');
  if (backBtn) {
    gsap.to(backBtn, { x: 0, duration: 0.2, ease: 'power2.inOut' });
  }
  const dayHeader = e.target.closest('.day-card-header');
  if (dayHeader) {
    const card = dayHeader.closest('.day-card');
    if (card) gsap.to(card, { boxShadow: '0 1px 3px rgba(0,0,0,0.04)', duration: 0.2 });
  }
}, true);

// Click ripple effect for primary buttons
document.body.addEventListener('click', (e) => {
  const btn = e.target.closest('.btn-primary');
  if (btn && !btn.disabled) {
    gsap.fromTo(btn, { scale: 0.97 }, { scale: 1, duration: 0.3, ease: 'elastic.out(1, 0.5)' });
  }
}, true);

// ============================================================
// GSAP: Card focus glow
// ============================================================
document.querySelectorAll('textarea, input[type="text"], input[type="password"]').forEach((input) => {
  input.addEventListener('focus', () => {
    const card = input.closest('.card');
    if (card) gsap.to(card, { borderColor: 'var(--border-hover)', duration: 0.3 });
  });
  input.addEventListener('blur', () => {
    const card = input.closest('.card');
    if (card) gsap.to(card, { borderColor: 'var(--border)', duration: 0.3 });
  });
});

// ============================================================
// GSAP: Mode indicator sliding pill
// ============================================================
function positionModeIndicator(container, animate = true) {
  const indicator = container.querySelector('.mode-indicator');
  if (!indicator) return;
  const checked = container.querySelector('input[type="radio"]:checked');
  if (!checked) return;
  const label = checked.closest('label');
  if (!label) return;

  const left = label.offsetLeft;
  const width = label.offsetWidth;

  if (animate) {
    gsap.to(indicator, { left, width, duration: 0.3, ease: 'power2.inOut' });
  } else {
    gsap.set(indicator, { left, width });
  }
}

// Initialize all mode indicators
document.querySelectorAll('.mode-options').forEach((container) => {
  // Set initial position after layout
  requestAnimationFrame(() => positionModeIndicator(container, false));

  // Animate on radio change
  container.querySelectorAll('input[type="radio"]').forEach((radio) => {
    radio.addEventListener('change', () => positionModeIndicator(container, true));
  });
});

// Reposition on resize
window.addEventListener('resize', () => {
  document.querySelectorAll('.mode-options').forEach((c) => positionModeIndicator(c, false));
});
