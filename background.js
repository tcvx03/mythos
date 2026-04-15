// Open the side panel when the extension icon is clicked
chrome.action.onClicked.addListener((tab) => {
  chrome.sidePanel.open({ tabId: tab.id });
});

// ---------------------------------------------------------------------------
// Handle API calls from the side panel.
// Gemini doesn't support CORS from browser contexts, so we proxy it here
// through the service worker which has no CORS restrictions.
// Claude does support browser CORS (with the special header), but we route
// it through here too for consistency and to keep API keys out of the panel.
// ---------------------------------------------------------------------------
chrome.runtime.onMessage.addListener((message, sender, sendResponse) => {
  if (message.type === 'CALL_AI_API') {
    handleAICall(message).then(sendResponse).catch((err) => {
      sendResponse({ error: err.message || 'Unknown error' });
    });
    return true; // keep channel open for async response
  }
});

async function handleAICall({ provider, apiKey, prompt, system, model, reasoning, maxTokens }) {
  if (provider === 'claude') {
    return await callClaude(apiKey, prompt, system, model, reasoning, maxTokens);
  } else if (provider === 'gemini') {
    return await callGemini(apiKey, prompt, system, model, reasoning, maxTokens);
  } else {
    throw new Error('Unknown provider: ' + provider);
  }
}

const CLAUDE_THINKING_BUDGETS = { low: 4096, medium: 10000, high: 24576 };

async function callClaude(apiKey, prompt, system, model, reasoning, maxTokens) {
  const selectedModel = model || 'claude-sonnet-4-6';
  const budget = CLAUDE_THINKING_BUDGETS[reasoning];

  const effectiveMax = maxTokens || 16000;
  const body = {
    model: selectedModel,
    max_tokens: budget ? Math.max(effectiveMax, budget + 1024) : effectiveMax,
    messages: [{ role: 'user', content: prompt }],
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
  });

  if (!res.ok) {
    const err = await res.json().catch(() => ({}));
    throw new Error(err?.error?.message || `Claude API error ${res.status}`);
  }

  const data = await res.json();
  // With thinking enabled, the text block may not be the first content block
  const textBlock = data.content.find(b => b.type === 'text');
  return { text: textBlock ? textBlock.text : '' };
}

const GEMINI_THINKING_BUDGETS = { low: 4096, medium: 16384, high: 32768 };

async function callGemini(apiKey, prompt, system, model, reasoning, maxTokens) {
  const selectedModel = model || 'gemini-3-flash-preview';
  const url = `https://generativelanguage.googleapis.com/v1beta/models/${selectedModel}:generateContent?key=${apiKey}`;
  const budget = GEMINI_THINKING_BUDGETS[reasoning];

  const body = {
    contents: [{ role: 'user', parts: [{ text: prompt }] }],
    generationConfig: { maxOutputTokens: Math.min(maxTokens || 16000, 65536), temperature: 0.7 },
  };

  if (system) {
    body.systemInstruction = { parts: [{ text: system }] };
  }
  if (budget) {
    body.generationConfig.thinkingConfig = { thinkingBudget: budget };
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

  const data = await res.json();
  return { text: data.candidates[0].content.parts[0].text };
}
