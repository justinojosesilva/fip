import websocket
import time
import logging

logger = logging.getLogger("websocket")

class WebSocketClient:
    def __init__(self, url, on_message):
        self.url = url
        self.on_message = on_message

    def run(self):
        
        retry = 1
        
        while True:
            try:
                ws = websocket.WebSocketApp(
                    self.url,
                    on_message=self.on_message,
                    on_error=self._on_error,
                    on_close=self._on_close
                )
                ws.on_open = self._on_open
                logger.info(f"Connecting to WebSocket: {self.url}")
                ws.run_forever(
                  ping_interval=20,
                  ping_timeout=10
                )
            except Exception as e:
                logger.error(f"WebSocket connection error: {e}")
                
            sleep = min(60, retry * 2)
            
            logger.warning(f"Reconnecting in {sleep} seconds...")
            time.sleep(sleep)
            retry += 1
            
    def _on_message(self, ws, message):
        logger.debug(f"Received message: {message}")
        self.on_message(ws, message)

    def _on_error(self, ws, error):
        logger.error(f"WebSocket error: {error}")

    def _on_close(self, ws, code, msg):
        logger.warning(f"WebSocket connection closed: {code} - {msg}")
      
    def _on_open(self, ws):
        logger.info("WebSocket connection opened")