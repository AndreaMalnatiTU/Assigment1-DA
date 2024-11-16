from typing import List, Dict, Tuple
import asyncio
import random
from cs4545.system.da_types import DistributedAlgorithm, message_wrapper
from ipv8.messaging.payload_dataclass import dataclass
from ipv8.types import Peer
from ipv8.community import CommunitySettings

@dataclass(msg_id=4)
class DolevMessage:
    source: int        # The source node of the message
    content: str       # The content of the message
    path: List[int]    # The sequence of nodes the message has traversed

class DolevAlgorithm(DistributedAlgorithm): 
    def __init__(self, settings: CommunitySettings, f=1, timeout=5) -> None:
        super().__init__(settings)
        self.messages: Dict[Tuple[int, str], Dict[str, any]] = {}  # Tracks message states and paths
        self.f = f  # Byzantine fault tolerance parameter
        self.timeout = timeout  # Timeout for node inactivity
        self.timer_task = None  # Reference to the timer task
        self.add_message_handler(DolevMessage, self.on_receive)

    async def start_timer(self):
        # Starts or restarts the inactivity timer
        if self.timer_task:
            self.timer_task.cancel()  # Cancel any existing timer
        self.timer_task = asyncio.create_task(self._timer())  # Start a new timer

    async def _timer(self):
        # Handles timeout for node inactivity
        try:
            await asyncio.sleep(self.timeout)  # Wait for the timeout duration
            self.append_output(f"[Node {self.node_id}] Timeout expired. Stopping.")
            self.stop()  # Stop the node
        except asyncio.CancelledError:
            pass  # Timer was reset or canceled

    def reset_timer(self):
        # Resets the inactivity timer
        asyncio.create_task(self.start_timer())

    async def on_start(self):
        # Called when the node starts
        if self.node_id in [0, 4, 7]:  # Nodes 0, 4, and 7 act as simultaneous senders
            await self.broadcast_message(f"Hello from Node {self.node_id}")
        self.reset_timer()  # Initialize the timer

    async def broadcast_message(self, content: str):
        # Broadcasts a message to all neighbors
        message_id = (self.node_id, content)
        self.messages[message_id] = {"delivered": False, "paths": set()}  # Initialize state and paths
        self.append_output(f"[Node {self.node_id}] Broadcasting message '{content}'")
        
        # Send the message to all peers in parallel
        tasks = []
        for peer in self.get_peers():
            tasks.append(self.send_with_delay(peer, DolevMessage(self.node_id, content, [])))
        
        await asyncio.gather(*tasks)  # Execute all tasks concurrently
        self.deliver_content(message_id)
        self.reset_timer()  # Reset the timer after broadcasting

    async def send_with_delay(self, peer: Peer, message: DolevMessage):
        # Sends a message with a random delay to simulate a real network
        await asyncio.sleep(random.uniform(0.1, 0.5))  # Delay between 100ms and 500ms
        self.ez_send(peer, message)
        self.append_output(f"[Node {self.node_id}] Sent message '{message.content}' to neighbor {self.node_id_from_peer(peer)}")

    @message_wrapper(DolevMessage)
    async def on_receive(self, peer: Peer, payload: DolevMessage):
        # Handles receiving a message
        if self.node_id == 6:  # Byzantine node 6 ignores all messages
            #self.append_output(f"[Node {self.node_id}] Ignoring message '{payload.content}' (Byzantine behavior)")
            #return
            payload.content = "Corrupted message"  # Corrupt the message content
        self.reset_timer()  # Reset the inactivity timer

        message_id = (payload.source, payload.content)
        sender_id = self.node_id_from_peer(peer)

        self.append_output(f"[Node {self.node_id}] Received message '{payload.content}' from Node {sender_id}. Path: {payload.path}")

        # Discard messages with cycles
        if self.node_id in payload.path:
            self.append_output(f"[Node {self.node_id}] Discarded message '{payload.content}' due to cycle in path: {payload.path}")
            return
        
        # Check for internal cycles within the path
        if len(set(payload.path)) < len(payload.path):  # Duplicate nodes indicate a cycle
            self.append_output(f"[Node {self.node_id}] Discarded message '{payload.content}' due to internal cycle in path: {payload.path}")
            return

        # Initialize message state if not present
        if message_id not in self.messages:
            self.messages[message_id] = {"delivered": False, "paths": set()}
            self.append_output(f"[Node {self.node_id}] Initialized state for message '{payload.content}' from source {payload.source}")

        # Construct the new path
        new_path = tuple(payload.path + [sender_id])

        # Check disjointness of paths, ignoring source and destination
        def exclude_source_dest(path):
            return path[1:-1] if len(path) > 2 else []

        if not all(self.are_disjoint(exclude_source_dest(new_path), exclude_source_dest(existing_path))
                   for existing_path in self.messages[message_id]["paths"]):
            self.forward_message(payload, new_path)
            return

        # Add the new path if valid
        self.messages[message_id]["paths"].add(new_path)
        self.append_output(f"[Node {self.node_id}] Added path: {new_path} for message '{payload.content}'")

        # Deliver the message if f+1 disjoint paths are received
        if len(self.messages[message_id]["paths"]) >= self.f + 1 and not self.messages[message_id]["delivered"]:
            self.deliver_content(message_id)

        # Forward the message to neighbors
        self.forward_message(payload, new_path)

    def forward_message(self, payload: DolevMessage, new_path: Tuple[int]):
        # Forwards the message to neighbors not in the path
        for neighbor in self.get_peers():
            neighbor_id = self.node_id_from_peer(neighbor)
            if neighbor_id not in new_path:
                asyncio.create_task(self.send_with_delay(neighbor, DolevMessage(payload.source, payload.content, list(new_path))))

    def deliver_content(self, message_id: Tuple[int, str]):
        # Delivers the message to the application
        if self.messages[message_id]["delivered"]: 
            return
        self.messages[message_id]["delivered"] = True 
        source, content = message_id
        self.append_output(f"[Node {self.node_id}] Delivered message '{content}' from source {source}.")
        print(f"[Node {self.node_id}] Delivered message '{content}' from source {source}.")

    def are_disjoint(self, path1: List[int], path2: List[int]) -> bool:
        # Checks if two paths are disjoint
        return not (set(path1) & set(path2))
