from fastapi import APIRouter, HTTPException, Depends, Query
from typing import List, Optional
from models import Task, TaskCreate, Priority
from config import FirebaseConfig, get_firebase_config
from openai import OpenAI
from pydantic import BaseModel
import json
from datetime import datetime, timedelta

client = OpenAI(api_key="xxx", base_url="https://api.sambanova.ai/v1")

class ModelOutput(BaseModel):
    document_id: str
    new_priority: Priority

router = APIRouter()

@router.get("/reorder_task", response_model=dict)
async def reorder_tasks(firebase: FirebaseConfig = Depends(get_firebase_config)):
    """Reorder tasks using AI based on attributes"""
    PROMPT = """
    # Task Prioritization System

    You are a professional task management assistant. Your role is to help prioritize tasks by analyzing their attributes and assigning appropriate priority levels (Low, Medium, or High).

    ## Input Format
    You will receive tasks in the following format:
    ```
    [
        {
            "title": string,
            "dueDate": datetime,
            "priority": Priority (Low/Medium/High),
            "description": string,
            "source": string,
            "document_id": string
        }
    ]
    ```

    ## Prioritization Rules
    1. Due Date Assessment:
    - Tasks due within 24 hours → High Priority
    - Tasks due within 1 week → Medium Priority
    - Tasks due beyond 1 week → Low Priority

    2. Description Analysis:
    - If description contains urgent keywords ("ASAP", "urgent", "immediate") → Increase priority by one level
    - If description starts with "#" → Consider as a header and maintain current priority
    
    3. Source Consideration:
    - Maintain consistent priority assessment regardless of source
    - Document ID is for reference only and should not affect priority

    ## Example Input:
    ```
    [
        {
            "title"="Test"
            "dueDate"="2012-04-23T18:25:43.511Z"
            "priority"="Medium"
            "description"='# Test Description'
            "source"='abca'
            "document_id"='735a215af7b94a2bb86b79769ada3218'
        },
        {
            "title"="Test2"
            "dueDate"="2029-04-23T18:25:43.511Z"
            "priority"="Medium"
            "description"='# Test Description'
            "source"='abca'
            "document_id"='735a215af7b94a2bb86b79769ada3218'
        }
    ]
    ```

    ## Expected Output:
    Return the task with an updated priority level and brief explanation:
    ```
    {
        "document_id": "735a215af7b94a2bb86b79769ada3218",
        "title": "Test",
        "updated_priority": "Low",
        "explanation": "Priority maintained as Low due to: non-urgent description, distant due date, and no priority-affecting keywords found."
    }
    ```

    ## Additional Instructions:
    1. Always analyze the complete task before assigning priority
    2. Provide brief explanation for priority changes
    3. Consider the original priority as a baseline
    4. Never increase priority beyond High or decrease below Low
    5. Treat missing attributes as neutral factors
    6. You are to ONLY output json with updated priority and explanation. Do not explain the rules in the output. Do not output any other information.

    Remember: Your goal is to help professionals manage their tasks effectively by assigning appropriate priorities based on the given criteria. Always maintain consistency in your prioritization logic.
    """

    # get all tasks from Firebase
    tasks_ref = firebase.db.collection("tasks")
    docs = tasks_ref.stream()
    tasks = [Task.from_dict(doc.to_dict()) for doc in docs]

    model="Meta-Llama-3.1-8B-Instruct"
    messages=[
        {"role": "system", "content": PROMPT},
    ]

    for task in tasks:
        messages.append({"role": "user", "content": f"{task.model_dump_json()}"})
    
    completion = client.beta.chat.completions.parse(model=model, messages=messages, temperature =  0.1,
        top_p = 0.1)

    event = completion.choices[0].message.content

    # search for json and ``` to get the json output

    event = event[event.find("{"):event.find("}")+1]
    
    event = json.loads(event)

    print(event)
    # Update task priorities in Firebase
    for output in event:
        doc_ref = firebase.db.collection("tasks").document(event.get("document_id"))
        doc = doc_ref.get()
        if not doc.exists:
            raise HTTPException(status_code=404, detail="Task not found")
        doc_ref.update({"priority": event.get("updated_priority")})

    # Implementation for AI-based reordering would go herey
    return {"success": True}

@router.get("/taskstats", response_model=dict)
async def get_task_stats(firebase: FirebaseConfig = Depends(get_firebase_config)):
    """Get statistics on task priorities"""
    tasks_ref = firebase.db.collection("tasks")
    docs = tasks_ref.get()
    stats = {"Low": 0, "Medium": 0, "High": 0, "Due_Today": 0, "Due_This_Week": 0, "Due_Later": 0, "Completed": 0}
    for doc in docs:
        task = Task.from_dict(doc.to_dict())
        if task.completed:
            stats["Completed"] += 1
        if task.dueDate.date() == datetime.now().date():
            stats["Due_Today"] += 1
        elif task.dueDate.date() <= (datetime.now() + timedelta(days=7)).date():
            stats["Due_This_Week"] += 1
        else:
            stats["Due_Later"] += 1
        stats[task.priority] += 1
    return stats

@router.get("/task", response_model=List[Task])
async def get_tasks(
    source: Optional[str] = Query(None, description="Filter tasks by source"),
    firebase: FirebaseConfig = Depends(get_firebase_config)
):
    """
    Get all tasks, optionally filtered by source
    
    Parameters:
    - source: Optional filter to get tasks from a specific source
    """
    tasks_ref = firebase.db.collection("tasks")
    
    # Apply source filter if provided
    if source:
        tasks_ref = tasks_ref.where("source", "==", source)
    
    try:
        docs = tasks_ref.stream()
        return [Task.from_dict(doc.to_dict()) for doc in docs]
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error retrieving tasks: {str(e)}")

@router.get("/task/{document_id}", response_model=Task)
async def get_task(document_id: str, firebase: FirebaseConfig = Depends(get_firebase_config)):
    """Get a specific task by ID"""
    doc_ref = firebase.db.collection("tasks").document(document_id)
    doc = doc_ref.get()
    if not doc.exists:
        raise HTTPException(status_code=404, detail="Task not found")
    return Task.from_dict(doc.to_dict())

@router.post("/task", response_model=dict)
async def create_task(task: TaskCreate, firebase: FirebaseConfig = Depends(get_firebase_config)):
    """Create a new task"""
    new_task = Task(**task.model_dump())
    doc_ref = firebase.db.collection("tasks").document(new_task.document_id)
    doc_ref.set(new_task.to_dict())
    return {"document_id": new_task.document_id}

@router.delete("/task/{document_id}", response_model=dict)
async def delete_task(document_id: str, firebase: FirebaseConfig = Depends(get_firebase_config)):
    """Delete a task"""
    doc_ref = firebase.db.collection("tasks").document(document_id)
    if not doc_ref.get().exists:
        raise HTTPException(status_code=404, detail="Task not found")
    doc_ref.delete()
    return {"success": True}

@router.post("/task/{document_id}/complete", response_model=dict)
async def complete_task(document_id: str, firebase: FirebaseConfig = Depends(get_firebase_config)):
    """Mark a task as completed"""
    doc_ref = firebase.db.collection("tasks").document(document_id)
    doc = doc_ref.get()
    if not doc.exists:
        raise HTTPException(status_code=404, detail="Task not found")
    doc_ref.update({"completed": True})
    return {"success": True}

@router.post("/task/{document_id}/priority", response_model=dict)
async def update_priority(document_id: str, new_priority: Priority, firebase: FirebaseConfig = Depends(get_firebase_config)):
    """Update the priority of a task"""
    doc_ref = firebase.db.collection("tasks").document(document_id)
    doc = doc_ref.get()
    if not doc.exists:
        raise HTTPException(status_code=404, detail="Task not found")
    doc_ref.update({"priority": new_priority})
    return {"success": True}