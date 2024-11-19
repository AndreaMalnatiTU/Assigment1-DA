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
    def __init__(self, settings: CommunitySettings, timeout=5) -> None:
        super().__init__(settings)
        self.messages: Dict[Tuple[int, str], Dict[str, any]] = {}  # Tracks message states and paths
        self.timeout = timeout  # Timeout for node inactivity
        self.timer_task = None  # Reference to the timer task
        self.add_message_handler(DolevMessage, self.on_receive)
        #print(f"{settings.__dict__.get('byzantine_nodes')}")
        self.byzantine_nodes = settings.__dict__.get("byzantine_nodes", [])  # List of byzantine nodes
        print(f"Byzantine nodes: {self.byzantine_nodes}")
        self.f = len(self.byzantine_nodes)  # Number of byzantine nodes


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
        if self.node_id in [0]:  # Nodes 0, 3, and 7 act as simultaneous senders
            await self.broadcast_message(f"Hello From {self.node_id}")
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
        """
        Handle received messages:
        1. Add the new path to the message record.
        2. Check if there are f+1 disjoint paths.
        3. Deliver the message if the condition is satisfied.
        4. Always forward the message.
            """
        self.reset_timer()

        if self.node_id == 6:
            payload.content = "corrupted message"
            payload.source = 34

        # Construct the message ID and new path
        message_id = (payload.source, payload.content)
        sender_id = self.node_id_from_peer(peer)
        new_path = tuple(payload.path + [sender_id])

        self.append_output(f"[DEBUG] [Node {self.node_id}] Received message: Source={payload.source}, "
                           f"Content='{payload.content}', Path={payload.path}, Sender={sender_id}")

        # **VALIDATION**
        if not self.is_valid_message(new_path, message_id):
            return

        # **MESSAGE MANAGEMENT**
        if message_id not in self.messages:
            self.messages[message_id] = {"delivered": False, "paths": set()}

        # Add the new path and check for disjoint paths
        if self.add_and_check_disjoint_paths(message_id, new_path) and not self.messages[message_id]["delivered"]:
            self.deliver_content(message_id)

        # **FORWARDING**
        self.forward_message(payload, new_path)


    def is_valid_message(self, path: Tuple[int], message_id: Tuple[int, str]) -> bool:
        """
        Validates the message path
        """
        if len(path) != len(set(path)):  # Check for cycles
            self.append_output(f"[DEBUG] [Node {self.node_id}] Invalid path: Cycles detected in path {path}")
            return False
        
        # Controlla se la sorgente del messaggio è il primo elemento del percorso
        expected_source = message_id[0]  # La sorgente specificata nel messaggio
        if path[0] != expected_source:
            self.append_output(f"[DEBUG] [Node {self.node_id}] Invalid path: Source {expected_source} is not the first element in path {path}")
            return False
        
        return True

    def add_and_check_disjoint_paths(self, message_id: Tuple[int, str], new_path: Tuple[int]) -> bool:
        """
        Add a new path and check if it forms f+1 disjoint paths with existing paths for the message.
        """
        # Retrieve existing paths for the message
        existing_paths = self.messages[message_id]["paths"]

        # Compare new path with all existing paths
        disjoint_count = 1
        for existing_path in existing_paths:
            if self.are_disjoint(existing_path[1:], new_path[1:]):
                disjoint_count += 1

        # Stop early if we already have f+1 disjoint paths
        if disjoint_count >= self.f + 1:
            return True

        # If no early exit, add the path to the set
        existing_paths.add(new_path)
        return False


    def forward_message(self, payload: DolevMessage, new_path: Tuple[int]):
        """
        Always forward the message to neighbors not in the current path.
        """
        for neighbor in self.get_peers():
            neighbor_id = self.node_id_from_peer(neighbor)
            if neighbor_id not in new_path:
                asyncio.create_task(self.send_with_delay(neighbor, DolevMessage(payload.source, payload.content, list(new_path))))

    def deliver_content(self, message_id: Tuple[int, str]):
        """
        Mark the message as delivered and log the action.
        """
        self.messages[message_id]["delivered"] = True
        source, content = message_id
        self.append_output(f"[DEBUG] [Node {self.node_id}] Delivered message '{content}' from source {source} "
                           f"with paths={self.messages[message_id]['paths']}")
        print(f"[Node {self.node_id}] Delivered message '{content}' from source {source}.")

    def are_disjoint(self, path1: List[int], path2: List[int]) -> bool:
        """
        Check if two paths are disjoint.
        """
        overlap = set(path1) & set(path2)
        return len(overlap) == 0

