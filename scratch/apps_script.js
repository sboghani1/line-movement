// Google Apps Script — paste this into Extensions → Apps Script in your Google Sheet
// Then deploy: Deploy → New deployment → Web app → Execute as: Me, Access: Anyone
// IMPORTANT: After pasting, you must create a NEW deployment (not edit existing)
// for changes to take effect.

function doGet(e) {
  var action = (e.parameter.action || '').trim();
  var callback = e.parameter.callback || '';

  if (action === 'add') {
    try {
      var data = JSON.parse(e.parameter.payload);
      var ss = SpreadsheetApp.getActiveSpreadsheet();
      var ws = ss.getSheetByName('picks_journal');

      // Get headers from row 3 (row 1-2 are preamble)
      var headers = ws.getRange(3, 1, 1, ws.getLastColumn()).getValues()[0];

      // Build row in header order
      var row = headers.map(function(h) {
        var key = h.trim().toLowerCase();
        return data[key] !== undefined ? data[key] : '';
      });

      // Append after last row
      ws.appendRow(row);

      var result = JSON.stringify({ status: 'ok' });
    } catch (err) {
      var result = JSON.stringify({ status: 'error', message: err.toString() });
    }

    // Return as JSONP if callback provided, otherwise plain JSON
    if (callback) {
      return ContentService.createTextOutput(callback + '(' + result + ')')
        .setMimeType(ContentService.MimeType.JAVASCRIPT);
    }
    return ContentService.createTextOutput(result)
      .setMimeType(ContentService.MimeType.JSON);
  }

  // Default health check
  return ContentService.createTextOutput(JSON.stringify({ status: 'ok', message: 'Picks Journal API is running' }))
    .setMimeType(ContentService.MimeType.JSON);
}

function doPost(e) {
  // Keep POST support as fallback
  try {
    var data;
    if (e.postData && e.postData.contents) {
      try {
        data = JSON.parse(e.postData.contents);
      } catch (_) {
        data = JSON.parse(e.parameter.payload);
      }
    } else if (e.parameter && e.parameter.payload) {
      data = JSON.parse(e.parameter.payload);
    } else {
      throw new Error('No data received');
    }

    var ss = SpreadsheetApp.getActiveSpreadsheet();
    var ws = ss.getSheetByName('picks_journal');
    var headers = ws.getRange(3, 1, 1, ws.getLastColumn()).getValues()[0];
    var row = headers.map(function(h) {
      var key = h.trim().toLowerCase();
      return data[key] !== undefined ? data[key] : '';
    });
    ws.appendRow(row);

    return ContentService.createTextOutput(JSON.stringify({ status: 'ok' }))
      .setMimeType(ContentService.MimeType.JSON);
  } catch (err) {
    return ContentService.createTextOutput(JSON.stringify({ status: 'error', message: err.toString() }))
      .setMimeType(ContentService.MimeType.JSON);
  }
}
