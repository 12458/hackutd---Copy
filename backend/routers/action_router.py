from fastapi import APIRouter, HTTPException, Depends
from typing import List
from models import ActionFlow, Node
from config import FirebaseConfig, get_firebase_config
from uuid import uuid4

router = APIRouter()

@router.get("/action", response_model=list[dict[str, str]])
async def get_actions(firebase: FirebaseConfig = Depends(get_firebase_config)):
    """Get all action IDs"""
    actions_ref = firebase.db.collection("rules")
    docs = actions_ref.stream()
    ret = []
    for doc in docs:
        doc_dict = doc.to_dict()
        ret.append({"action_id": doc.id, "name": doc_dict.get("name"), "description": doc_dict.get("description")})
    return ret

@router.get("/action/{action_id}/nodes", response_model=ActionFlow)
async def get_action_nodes(action_id: str, firebase: FirebaseConfig = Depends(get_firebase_config)):
    """Get nodes for a specific action"""
    doc_ref = firebase.db.collection("rules").document(action_id)
    doc = doc_ref.get()
    if not doc.exists:
        raise HTTPException(status_code=404, detail="Action not found")
    return ActionFlow.from_dict(doc.to_dict())

@router.post("/action/create", response_model=dict)
async def create_action(action_flow: ActionFlow, firebase: FirebaseConfig = Depends(get_firebase_config,)):
    """Create a new action flow"""
    doc_ref = firebase.db.collection("rules").document(action_flow.id)
    doc_ref.set(action_flow.model_dump())
    return {"action_id": action_flow.id}

@router.post("/action/{action_id}/nodesupdate", response_model=dict)
async def update_action_nodes(action_id: str, nodes: list[Node], start_node: Node, firebase: FirebaseConfig = Depends(get_firebase_config)):
    """Update nodes for an existing action"""
    doc_ref = firebase.db.collection("rules").document(action_id)
    # Update the start node
    doc_ref.update({"start_node": start_node.id})
    # Update the nodes
    doc_ref.update({"nodes": [node.model_dump() for node in nodes]})    
    return {"success": True}