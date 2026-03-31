"""
Random Radio FM — Play random live radio stations from around the world.
 
Trigger it, it plays. Say "next" to skip. Say "stop" to exit.
Uses httpx async streaming so voice commands work during playback.
"""
 
import asyncio
import random
import httpx
import requests
from src.agent.capability import MatchingCapability
from src.main import AgentWorker
from src.agent.capability_worker import CapabilityWorker
 
RADIO_BROWSER = "https://de1.api.radio-browser.info"
REQUEST_TIMEOUT = 10
 
VOICE_ID = "TxGEqnHWrfWFTfGW9XjX"
 
DJ_INTROS = [
    "You're live on Random Radio FM — broadcasting from your OpenHome with a nonstop mix of stations from every corner of the planet. No playlists, no algorithms, just pure random radio from wherever the signal lands. Let's spin the dial.",
    "And we're live — this is Random Radio FM, coming at you from OpenHome with a nonstop stream of live radio from random stations around the globe. No two sessions are ever the same.",
    "No genres. No borders. No idea what's next. Just live radio from around the world, straight through your OpenHome. Let's tune in.",
]
 
EXIT_WORDS = {"stop", "quit", "exit", "cancel", "end", "done", "goodbye", "enough"}
EXIT_PHRASES = ["turn off the radio", "stop radio", "radio off", "shut off the radio"]
SKIP_WORDS = {"skip", "next", "forward", "another", "different", "change"}
 
 
class RandomRadioCapability(MatchingCapability):
    worker: AgentWorker = None
    capability_worker: CapabilityWorker = None
 
    _playing: bool = False
    _pending_command: str = None
    _last_processed_msg: str = ""
 
    #{{register capability}}
 
    def call(self, worker: AgentWorker):
        try:
            self.worker = worker
            self.capability_worker = CapabilityWorker(self.worker)
            worker.editor_logging_handler.info("[RandomRadio] call()")
            self.worker.session_tasks.create(self.main_flow())
        except Exception as e:
            worker.editor_logging_handler.error(f"[RandomRadio] call() failed: {e}")
 
    # ── Command Detection ─────────────────────────────────────────────────────
 
    def _check_command(self, text):
        """Check text for radio commands. Returns 'stop', 'next', or None."""
        if not text:
            return None
        lower = text.lower().strip()
        words = set(lower.split())
 
        if len(lower.split()) > 6:
            return None
 
        if words & EXIT_WORDS or any(p in lower for p in EXIT_PHRASES):
            return "stop"
 
        if words & SKIP_WORDS:
            return "next"
 
        return None
 
    # ── Fetch Stations ────────────────────────────────────────────────────────
 
    def _fetch_random_stations(self, count: int = 30) -> list:
        try:
            resp = requests.get(
                f"{RADIO_BROWSER}/json/stations/topclick/{count * 5}",
                headers={"User-Agent": "OpenHome/1.0"},
                params={"hidebroken": "true"},
                timeout=REQUEST_TIMEOUT,
            )
            self.worker.editor_logging_handler.info(f"[RandomRadio] API → {resp.status_code}")
 
            if resp.status_code != 200:
                return []
 
            all_stations = resp.json()
            mp3_stations = []
            for s in all_stations:
                stream_url = s.get("url_resolved") or s.get("url", "")
                codec = s.get("codec", "").upper()
                if stream_url and codec == "MP3":
                    mp3_stations.append({
                        "title": s.get("name", "Unknown Station"),
                        "country": s.get("country", ""),
                        "stream_url": stream_url,
                    })
 
            self.worker.editor_logging_handler.info(
                f"[RandomRadio] {len(mp3_stations)} MP3 stations from {len(all_stations)} total"
            )
            random.shuffle(mp3_stations)
            return mp3_stations[:count]
 
        except Exception as e:
            self.worker.editor_logging_handler.error(f"[RandomRadio] Fetch error: {e}")
            return []
 
    # ── Async Audio Streaming ─────────────────────────────────────────────────
 
    async def _stream_audio(self, stream_response):
        """
        Stream audio via the streaming API using httpx async iteration.
        Event loop yields between chunks so voice commands work.
        Returns True if stream ended naturally, False if interrupted.
        """
        try:
            await self.capability_worker.stream_init()
 
            async for chunk in stream_response.aiter_bytes(chunk_size=25 * 1024):
                if not chunk:
                    continue
 
                # Stop check
                if self.worker.music_mode_stop_event.is_set() or not self._playing:
                    self.worker.editor_logging_handler.info("[RandomRadio] Stop event during stream")
                    await self.capability_worker.stream_end()
                    return False
 
                # Pause check
                while self.worker.music_mode_pause_event.is_set():
                    if self.worker.music_mode_stop_event.is_set() or not self._playing:
                        await self.capability_worker.stream_end()
                        return False
                    await asyncio.sleep(0.1)
 
                # Check for pending command from monitor
                if self._pending_command:
                    self.worker.editor_logging_handler.info("[RandomRadio] Command detected during stream")
                    await self.capability_worker.stream_end()
                    return False
 
                await self.capability_worker.send_audio_data_in_stream(chunk)
 
            await self.capability_worker.stream_end()
            return True
 
        except Exception as e:
            self.worker.editor_logging_handler.error(f"[RandomRadio] Stream error: {e}")
            try:
                await self.capability_worker.stream_end()
            except Exception:
                pass
            return False
 
    # ── Command Monitor ───────────────────────────────────────────────────────
 
    async def _monitor_commands(self):
        """Background task: watches message history during playback for commands."""
        while self._playing:
            try:
                history = self.capability_worker.get_full_message_history()[-3:]
                for msg in history:
                    content = msg.get("content", "")
                    if not content or content == self._last_processed_msg:
                        continue
                    if msg.get("role") != "user":
                        continue
 
                    cmd = self._check_command(content)
                    if cmd:
                        self._pending_command = cmd
                        self._last_processed_msg = content
                        self._playing = False
                        await self.capability_worker.send_interrupt_signal()
                        return
            except Exception:
                pass
            await self.worker.session_tasks.sleep(1.0)
 
    # ── Main Flow ─────────────────────────────────────────────────────────────
 
    async def main_flow(self):
        try:
            self.worker.editor_logging_handler.info("[RandomRadio] main_flow started")
 
            # Play the radio tag jingle
            await self.capability_worker.play_from_audio_file("Random_Radio_Tag.mp3")
 
            # Randomized DJ intro
            await self.capability_worker.text_to_speech(random.choice(DJ_INTROS), VOICE_ID)
 
            # Fetch stations
            stations = self._fetch_random_stations()
 
            if not stations:
                await self.capability_worker.speak(
                    "Couldn't reach the station directory right now. Try again later."
                )
                self.capability_worker.resume_normal_flow()
                return
 
            self.worker.editor_logging_handler.info(f"[RandomRadio] {len(stations)} stations queued")
            station_index = 0
 
            while station_index < len(stations):
                station = stations[station_index]
                title = station["title"]
                country = station["country"]
                stream_url = station["stream_url"]
 
                # Enter music mode
                self._playing = True
                self._pending_command = None
                self.worker.music_mode_event.set()
                await self.capability_worker.send_data_over_websocket("music-mode", {"mode": "on"})
 
                # Announce station
                await self.capability_worker.text_to_speech(
                    f"Now playing {title} from {country}.",
                    VOICE_ID
                )
 
                # Spawn command monitor
                monitor = self.worker.session_tasks.create(self._monitor_commands())
 
                self.worker.editor_logging_handler.info(
                    f"[RandomRadio] Streaming: {title} ({stream_url})"
                )
 
                # Stream with httpx
                completed = False
                try:
                    async with httpx.AsyncClient(timeout=None) as client:
                        async with client.stream("GET", stream_url, follow_redirects=True, headers={
                            "User-Agent": "OpenHome/1.0"
                        }) as stream_response:
                            self.worker.editor_logging_handler.info(
                                f"[RandomRadio] HTTP {stream_response.status_code}"
                            )
                            if stream_response.status_code == 200:
                                completed = await self._stream_audio(stream_response)
                            else:
                                self.worker.editor_logging_handler.info(
                                    f"[RandomRadio] Bad status, skipping"
                                )
                except Exception as e:
                    self.worker.editor_logging_handler.error(f"[RandomRadio] Connection error: {e}")
 
                # Cleanup
                self._playing = False
                try:
                    monitor.cancel()
                except Exception:
                    pass
                await self.capability_worker.send_data_over_websocket("music-mode", {"mode": "off"})
                self.worker.music_mode_event.clear()
 
                # Handle result
                if self._pending_command == "stop":
                    await self.capability_worker.speak("Radio off.")
                    break
                else:
                    # Next station — whether skipped, stream died, or connection failed
                    station_index += 1
                    continue
 
            # Ran out of stations
            if station_index >= len(stations):
                await self.capability_worker.text_to_speech(
                    "That's all the stations. Heading back.", VOICE_ID
                )
 
        except Exception as e:
            self.worker.editor_logging_handler.error(f"[RandomRadio] Error: {e}")
            await self.capability_worker.speak("Something went wrong. Heading back.")
        finally:
            self._playing = False
            try:
                await self.capability_worker.send_data_over_websocket("music-mode", {"mode": "off"})
                self.worker.music_mode_event.clear()
            except Exception:
                pass
            self.worker.editor_logging_handler.info("[RandomRadio] Exiting")
            self.capability_worker.resume_normal_flow()