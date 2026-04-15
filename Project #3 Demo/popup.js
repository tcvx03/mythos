const btn = document.getElementById('typeBtn');
const statusEl = document.getElementById('status');
const textInput = document.getElementById('textInput');

btn.addEventListener('click', async () => {
  const text = textInput.value;

  if (!text) {
    statusEl.textContent = 'Please enter some text first.';
    return;
  }

  const [tab] = await chrome.tabs.query({ active: true, currentWindow: true });

  if (!tab.url || !tab.url.includes('docs.google.com/document')) {
    statusEl.textContent = 'Navigate to a Google Docs document first.';
    return;
  }

  btn.disabled = true;
  statusEl.textContent = 'Typing...';

  try {
    const response = await chrome.tabs.sendMessage(tab.id, {
      type: 'TYPE_TEXT',
      text: text
    });

    if (response && response.success) {
      statusEl.textContent = 'Done!';
    } else {
      statusEl.textContent = 'Could not find the editor. Click inside the doc first, then try again.';
    }
  } catch (err) {
    statusEl.textContent = 'Error: Reload the Google Doc page, then try again.';
    console.error(err);
  } finally {
    btn.disabled = false;
  }
});
