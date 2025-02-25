
def format_genesis_g_sheets(columns):
    return [
                # Set row heights
                {
                    "updateDimensionProperties": {
                        "range": {
                            "sheetId": 0,
                            "dimension": "ROWS",
                            "startIndex": 0,
                            "endIndex": len(columns)
                        },
                        "properties": {
                            "pixelSize": 63
                        },
                        "fields": "pixelSize"
                    }
                },
                # Set column width
                {
                    "updateDimensionProperties": {
                        "range": {
                            "sheetId": 0,
                            "dimension": "COLUMNS",
                            "startIndex": 0,
                            "endIndex": len(columns[0])
                        },
                        "properties": {
                            "pixelSize": 300
                        },
                        "fields": "pixelSize"
                    }
                },
                # Format header row with deep blue background
                {
                    "updateCells": {
                        "range": {
                            "sheetId": 0,
                            "startRowIndex": 0,
                            "endRowIndex": 1,
                            "startColumnIndex": 0,
                            "endColumnIndex": len(columns[0])
                        },
                        "rows": [{
                            "values": [{
                                "userEnteredFormat": {
                                    "backgroundColor": {"red": 0.27, "green": 0.51, "blue": 0.71},  # Adjusted to match image blue
                                    "textFormat": {
                                        "foregroundColor": {"red": 1.0, "green": 1.0, "blue": 1.0},  # White text
                                        "bold": True
                                    },
                                    "horizontalAlignment": "CENTER",
                                    "verticalAlignment": "MIDDLE"
                                }
                            }] * len(columns[0])
                        }],
                        "fields": "userEnteredFormat(backgroundColor,textFormat,horizontalAlignment,verticalAlignment)"
                    }
                },
                # Add alternating row colors (white and light orange)
                {
                    "addBanding": {
                        "bandedRange": {
                            "range": {
                                "sheetId": 0,
                                "startRowIndex": 1,
                                "endRowIndex": len(columns),
                                "startColumnIndex": 0,
                                "endColumnIndex": len(columns[0])
                            },
                            "rowProperties": {
                                "firstBandColor": {"red": 1.0, "green": 1.0, "blue": 1.0},  # White
                                "secondBandColor": {"red": 1.0, "green": 0.95, "blue": 0.9}  # Very light orange
                            }
                        }
                    }
                },
                # Add borders in orange
                {
                    "updateBorders": {
                        "range": {
                            "sheetId": 0,
                            "startRowIndex": 0,
                            "endRowIndex": len(columns),
                            "startColumnIndex": 0,
                            "endColumnIndex": len(columns[0])
                        },
                        "top": {"style": "SOLID", "width": 2, "color": {"red": 1.0, "green": 0.65, "blue": 0.0}},  # Orange
                        "bottom": {"style": "SOLID", "width": 2, "color": {"red": 1.0, "green": 0.65, "blue": 0.0}},
                        "left": {"style": "SOLID", "width": 2, "color": {"red": 1.0, "green": 0.65, "blue": 0.0}},
                        "right": {"style": "SOLID", "width": 2, "color": {"red": 1.0, "green": 0.65, "blue": 0.0}},
                        "innerHorizontal": {"style": "SOLID", "color": {"red": 1.0, "green": 0.65, "blue": 0.0}},
                        "innerVertical": {"style": "SOLID", "color": {"red": 1.0, "green": 0.65, "blue": 0.0}}
                    }
                },
                # Freeze header row
                {
                    "updateSheetProperties": {
                        "properties": {
                            "sheetId": 0,
                            "gridProperties": {
                                "frozenRowCount": 1
                            }
                        },
                        "fields": "gridProperties.frozenRowCount"
                    }
                },
                # Enable text wrapping
                {
                    "repeatCell": {
                        "range": {
                            "sheetId": 0,
                            "startRowIndex": 0,
                            "endRowIndex": len(columns),
                            "startColumnIndex": 0,
                            "endColumnIndex": len(columns[0])
                        },
                        "cell": {
                            "userEnteredFormat": {
                                "wrapStrategy": "WRAP"
                            }
                        },
                        "fields": "userEnteredFormat.wrapStrategy"
                    }
                }
            ]
