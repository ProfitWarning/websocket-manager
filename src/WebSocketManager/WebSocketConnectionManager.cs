using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Net.WebSockets;
using System.Threading;
using System.Threading.Tasks;

namespace WebSocketManager
{
    public class WebSocketConnectionManager
    {
        private ConcurrentDictionary<string, WebSocket> _sockets = new ConcurrentDictionary<string, WebSocket>();
        private ConcurrentDictionary<string, List<string>> _groups = new ConcurrentDictionary<string, List<string>>();

        public WebSocket GetSocketById(string id)
        {
            return _sockets.TryGetValue(id, out WebSocket ret) ? ret : null;
        }

        public bool SocketExists(string id)
        {
            return _sockets.ContainsKey(id);
        }

        public ConcurrentDictionary<string, WebSocket> GetAll()
        {
            return _sockets;
        }

        public List<string> GetAllFromGroup(string GroupID)
        {
            if (_groups.ContainsKey(GroupID))
            {
                var list = _groups[GroupID];
                lock (list)
                {
                    return new List<string>(list);
                }
            }

            return default(List<string>);
        }

        public string GetId(WebSocket socket)
        {
            return _sockets.FirstOrDefault(p => p.Value == socket).Key;
        }

        public string AddSocket(WebSocket socket)
        {
            string id = CreateConnectionId();

            _sockets.TryAdd(id, socket);

            return id;
        }

        public void AddToGroup(string socketID, string groupID)
        {
            if (_groups.ContainsKey(groupID))
            {
                var list = _groups[groupID];
                lock (list)
                {
                    list.Add(socketID);
                }
                _groups[groupID] = list;

                return;
            }

            _groups.TryAdd(groupID, new List<string> { socketID });
        }

        public void RemoveFromGroup(string socketID, string groupID)
        {
            if (_groups.ContainsKey(groupID))
            {
                var list = _groups[groupID];
                lock (list)
                {
                    list.Remove(socketID);
                }
                _groups[groupID] = list;

                return;
            }
        }

        public async Task RemoveSocket(string id, WebSocketCloseStatus closeStatus = WebSocketCloseStatus.NormalClosure)
        {
            WebSocket socket;
            if (_sockets.TryRemove(id, out socket))
            {

                try
                {
                    await socket.CloseAsync(closeStatus: closeStatus,
                                            statusDescription: "Closed by the WebSocketManager",
                                            cancellationToken: CancellationToken.None).ConfigureAwait(false);
                }
                catch (WebSocketException) { }
            }            
        }

        private string CreateConnectionId()
        {
            return Guid.NewGuid().ToString();
        }
    }
}
