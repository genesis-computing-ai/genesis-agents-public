import * as vscode from 'vscode';

// Bottom panel view
class ServerAdminPanelView {
    public static currentPanel: vscode.WebviewPanel | undefined;

    public static createOrShow(extensionUri: vscode.Uri) {
        const column = vscode.ViewColumn.Beside;

        if (ServerAdminPanelView.currentPanel) {
            ServerAdminPanelView.currentPanel.reveal(column);
            return;
        }

        const panel = vscode.window.createWebviewPanel(
            'serverAdmin',
            'Server Admin',
            { viewColumn: vscode.ViewColumn.Two, preserveFocus: true },
            {
                enableScripts: true,
                retainContextWhenHidden: true,
                localResourceRoots: [extensionUri]
            }
        );

        panel.webview.html = getBottomPanelContent();

        panel.onDidDispose(
            () => {
                ServerAdminPanelView.currentPanel = undefined;
            },
            null,
            []
        );

        ServerAdminPanelView.currentPanel = panel;
    }

    public static kill() {
        ServerAdminPanelView.currentPanel?.dispose();
        ServerAdminPanelView.currentPanel = undefined;
    }
}

// Sidebar view provider
class ServerConfigViewProvider implements vscode.WebviewViewProvider {
    private _view?: vscode.WebviewView;

    constructor(private readonly _extensionUri: vscode.Uri) {}

    public resolveWebviewView(
        webviewView: vscode.WebviewView,
        context: vscode.WebviewViewResolveContext,
        _token: vscode.CancellationToken,
    ) {
        this._view = webviewView;

        webviewView.webview.options = {
            enableScripts: true,
            localResourceRoots: [this._extensionUri]
        };

        webviewView.webview.html = this._getWebviewContent();
        
        webviewView.webview.onDidReceiveMessage(async (message) => {
            switch (message.command) {
                case 'openBottomPanel':
                    vscode.commands.executeCommand('serverAdmin.openPanel');
                    break;
                case 'showQuickActions':
                    await this._showQuickActions();
                    break;
                case 'showServerStatus':
                    await this._checkServerStatus();
                    break;
            }
        });
    }

    private async _showQuickActions() {
        const action = await vscode.window.showQuickPick([
            'Restart Server',
            'Clear Logs',
            'Update Configuration',
            'View Metrics'
        ], {
            placeHolder: 'Select an action'
        });

        switch (action) {
            case 'Restart Server':
                await vscode.window.showInformationMessage('Server restart initiated...');
                break;
            case 'Clear Logs':
                await vscode.window.showInformationMessage('Clearing logs...');
                break;
            case 'Update Configuration':
                vscode.commands.executeCommand('serverAdmin.openPanel');
                break;
            case 'View Metrics':
                vscode.commands.executeCommand('serverAdmin.openPanel');
                break;
        }
    }

    private async _checkServerStatus() {
        const status = {
            status: 'Running',
            uptime: '3 days, 2 hours',
            memory: '1.2GB/4GB',
            cpu: '23%'
        };

        if (this._view) {
            await this._view.webview.postMessage({ 
                command: 'updateStatus', 
                status 
            });
        }
    }

    private _getWebviewContent() {
        return `<!DOCTYPE html>
        <html>
        <head>
            <style>
                body { 
                    padding: 10px; 
                    color: var(--vscode-foreground);
                    font-family: var(--vscode-font-family);
                }
                .nav-item {
                    padding: 8px;
                    cursor: pointer;
                    margin-bottom: 8px;
                    background: var(--vscode-button-background);
                    color: var(--vscode-button-foreground);
                    border: none;
                    border-radius: 2px;
                    width: 100%;
                    text-align: left;
                }
                .nav-item:hover {
                    background: var(--vscode-button-hoverBackground);
                }
                .status-container {
                    margin-top: 20px;
                    padding: 10px;
                    background: var(--vscode-editor-background);
                    border: 1px solid var(--vscode-panel-border);
                }
                .status-item {
                    margin: 5px 0;
                    display: flex;
                    justify-content: space-between;
                }
            </style>
        </head>
        <body>
            <button class="nav-item" onclick="openBottomPanel()">Open Details Panel</button>
            <button class="nav-item" onclick="showQuickActions()">Quick Actions</button>
            <button class="nav-item" onclick="showServerStatus()">Check Server Status</button>
            
            <div id="status-container" class="status-container" style="display: none;">
                <h3>Server Status</h3>
                <div class="status-item">
                    <span>Status:</span>
                    <span id="status-value">-</span>
                </div>
                <div class="status-item">
                    <span>Uptime:</span>
                    <span id="uptime-value">-</span>
                </div>
                <div class="status-item">
                    <span>Memory:</span>
                    <span id="memory-value">-</span>
                </div>
                <div class="status-item">
                    <span>CPU:</span>
                    <span id="cpu-value">-</span>
                </div>
            </div>
            
            <script>
                const vscode = acquireVsCodeApi();
                
                function openBottomPanel() {
                    vscode.postMessage({ command: 'openBottomPanel' });
                }
                
                function showQuickActions() {
                    vscode.postMessage({ command: 'showQuickActions' });
                }
                
                function showServerStatus() {
                    vscode.postMessage({ command: 'showServerStatus' });
                }

                window.addEventListener('message', event => {
                    const message = event.data;
                    switch (message.command) {
                        case 'updateStatus':
                            const status = message.status;
                            document.getElementById('status-container').style.display = 'block';
                            document.getElementById('status-value').textContent = status.status;
                            document.getElementById('uptime-value').textContent = status.uptime;
                            document.getElementById('memory-value').textContent = status.memory;
                            document.getElementById('cpu-value').textContent = status.cpu;
                            break;
                    }
                });
            </script>
        </body>
        </html>`;
    }
}

function getBottomPanelContent() {
    return `<!DOCTYPE html>
        <html>
        <head>
            <style>
                body {
                    font-family: var(--vscode-font-family);
                    padding: 10px;
                    color: var(--vscode-foreground);
                }
                .tab-container {
                    display: flex;
                    border-bottom: 1px solid var(--vscode-panel-border);
                    margin-bottom: 20px;
                }
                .tab {
                    padding: 8px 16px;
                    cursor: pointer;
                    border: none;
                    background: none;
                    color: var(--vscode-foreground);
                }
                .tab.active {
                    border-bottom: 2px solid var(--vscode-activityBarBadge-background);
                }
                .panel {
                    display: none;
                }
                .panel.active {
                    display: block;
                }
                button {
                    background: var(--vscode-button-background);
                    color: var(--vscode-button-foreground);
                    border: none;
                    padding: 8px;
                    cursor: pointer;
                }
                input, select {
                    background: var(--vscode-input-background);
                    color: var(--vscode-input-foreground);
                    border: 1px solid var(--vscode-input-border);
                    padding: 4px;
                    margin: 4px 0;
                }
            </style>
        </head>
        <body>
            <div class="tab-container">
                <button class="tab active" onclick="showPanel('config')">Configuration</button>
                <button class="tab" onclick="showPanel('monitoring')">Monitoring</button>
                <button class="tab" onclick="showPanel('logs')">Logs</button>
            </div>

            <div id="config" class="panel active">
                <h3>Detailed Configuration</h3>
                <div>
                    <label>Host:</label>
                    <input type="text" id="host">
                    <label>Port:</label>
                    <input type="number" id="port">
                    <label>Environment:</label>
                    <select id="env">
                        <option value="dev">Development</option>
                        <option value="staging">Staging</option>
                        <option value="prod">Production</option>
                    </select>
                    <button onclick="saveConfig()">Save</button>
                </div>
            </div>

            <div id="monitoring" class="panel">
                <h3>Monitoring</h3>
                <div id="metrics"></div>
            </div>

            <div id="logs" class="panel">
                <h3>Logs</h3>
                <div id="log-viewer" style="height: 300px; overflow-y: auto;"></div>
            </div>

            <script>
                const vscode = acquireVsCodeApi();

                function showPanel(panelId) {
                    document.querySelectorAll('.panel').forEach(p => p.classList.remove('active'));
                    document.querySelectorAll('.tab').forEach(t => t.classList.remove('active'));
                    document.getElementById(panelId).classList.add('active');
                    event.target.classList.add('active');
                }

                function saveConfig() {
                    const config = {
                        host: document.getElementById('host').value,
                        port: document.getElementById('port').value,
                        env: document.getElementById('env').value
                    };
                    vscode.postMessage({ command: 'saveConfig', config });
                }
            </script>
        </body>
        </html>`;
}

export function activate(context: vscode.ExtensionContext) {
    // Register sidebar view
    const sidebarProvider = new ServerConfigViewProvider(context.extensionUri);
    context.subscriptions.push(
        vscode.window.registerWebviewViewProvider('serverAdmin.sidebar', sidebarProvider)
    );

    // Register command to open bottom panel
    let disposable = vscode.commands.registerCommand('serverAdmin.openPanel', () => {
        ServerAdminPanelView.createOrShow(context.extensionUri);
    });

    context.subscriptions.push(disposable);
}