import { useState, useCallback, useMemo } from 'react';
import { motion, Reorder, AnimatePresence, useDragControls } from 'framer-motion';
import TodoCard from '../components/TodoCard';
import TaskDetailModal from '../components/TaskDetailModal';
import Toast from '../components/Toast';
import ListSelector from '../components/ListSelector';
import { FiRefreshCw } from 'react-icons/fi';
import useIsMobile from '../hooks/useIsMobile';

function TodoPage() {
  const [todos, setTodos] = useState([
    { 
      id: '1', 
      title: 'Check HVAC System', 
      dueDate: 'Today', 
      priority: 'High',
      source: 'HVAC',
      description: '# HVAC System Check\n\n1. Check air filters\n2. Inspect cooling towers\n3. Test thermostats\n4. Check refrigerant levels\n\n**Note:** Document any unusual sounds or vibrations'
    },
    { 
      id: '2', 
      title: 'Inspect Fire Alarms', 
      dueDate: 'Tomorrow', 
      priority: 'High',
      source: 'Gas',
      description: '# Fire Alarm Inspection\n\n1. Check battery levels\n2. Test alarm systems\n3. Inspect fire sprinkler systems\n4. Ensure compliance with local fire codes' 
    },
    { 
      id: '3', 
      title: 'Check Water Pressure', 
      dueDate: 'Today', 
      priority: 'Medium',
      source: 'Water',
      description: '# Water System Check\n\n1. Check pressure levels\n2. Inspect pipes\n3. Test water quality\n4. Document findings' 
    },
    { 
      id: '4', 
      title: 'Elevator Maintenance', 
      dueDate: 'Next Week', 
      priority: 'Low',
      source: 'Elevator',
      description: '# Elevator Maintenance\n\n1. Check safety systems\n2. Test emergency calls\n3. Inspect cables\n4. Document maintenance' 
    },
    { 
      id: '5', 
      title: 'Monthly Safety Report', 
      dueDate: 'Today', 
      priority: 'Medium',
      source: 'Report',
      description: '# Safety Report\n\n1. Compile incidents\n2. Review safety measures\n3. Update procedures\n4. Submit report' 
    },
  ]);

  const [completedTasks, setCompletedTasks] = useState([]);
  const [removedTasks, setRemovedTasks] = useState([]);
  const [selectedList, setSelectedList] = useState('all');
  const [selectedTodo, setSelectedTodo] = useState(null);
  const [lastAction, setLastAction] = useState(null);
  const [showToast, setShowToast] = useState(false);
  const [removedTodo, setRemovedTodo] = useState(null);
  const isMobile = useIsMobile();

  const lists = ['HVAC', 'Water', 'Gas', 'Elevator', 'Report', 'Completed', 'Removed'];
  
  const filteredTodos = useMemo(() => {
    if (selectedList === 'Completed') return completedTasks;
    if (selectedList === 'Removed') return removedTasks;
    if (selectedList === 'all') return todos;
    return todos.filter(todo => todo.source === selectedList);
  }, [todos, selectedList, completedTasks, removedTasks]);

  const counts = useMemo(() => {
    const result = { 
      all: todos.length,
      Completed: completedTasks.length,
      Removed: removedTasks.length,
    };
    lists.forEach(list => {
      if (list !== 'Completed' && list !== 'Removed') {
        result[list] = todos.filter(todo => todo.source === list).length;
      }
    });
    return result;
  }, [todos, lists, completedTasks, removedTasks]);

  const handleComplete = useCallback((id) => {
    const todoToComplete = todos.find(todo => todo.id === id);
    if (todoToComplete) {
      const completedTask = {
        ...todoToComplete,
        completedAt: new Date().toISOString(),
      };
      setCompletedTasks(prev => [completedTask, ...prev]);
      setTodos(todos.filter(todo => todo.id !== id));
      setSelectedTodo(null);
      setLastAction('complete');
      setShowToast(true);
      setTimeout(() => setShowToast(false), 5000);
    }
  }, [todos]);

  const handleDelete = useCallback((id) => {
    const todoToRemove = todos.find(todo => todo.id === id);
    if (todoToRemove) {
      const removedTask = {
        ...todoToRemove,
        removedAt: new Date().toISOString(),
      };
      setRemovedTasks(prev => [removedTask, ...prev]);
      setTodos(todos.filter(todo => todo.id !== id));
      setSelectedTodo(null);
      setLastAction('delete');
      setShowToast(true);
      setTimeout(() => setShowToast(false), 5000);
    }
  }, [todos]);

  const handleRestore = useCallback((id, source) => {
    if (source === 'Completed') {
      const taskToRestore = completedTasks.find(task => task.id === id);
      if (taskToRestore) {
        const { completedAt, ...restoredTask } = taskToRestore;
        setTodos(prev => [...prev, restoredTask]);
        setCompletedTasks(prev => prev.filter(task => task.id !== id));
      }
    } else if (source === 'Removed') {
      const taskToRestore = removedTasks.find(task => task.id === id);
      if (taskToRestore) {
        const { removedAt, ...restoredTask } = taskToRestore;
        setTodos(prev => [...prev, restoredTask]);
        setRemovedTasks(prev => prev.filter(task => task.id !== id));
      }
    }
  }, [completedTasks, removedTasks]);

  const handlePriorityChange = (id, newPriority) => {
    setTodos(todos.map(todo => 
      todo.id === id ? { ...todo, priority: newPriority } : todo
    ));
  };

  const handleReorderWithAI = () => {
    const reorderedTodos = [...todos].sort((a, b) => {
      const priorityOrder = { High: 0, Medium: 1, Low: 2 };
      return priorityOrder[a.priority] - priorityOrder[b.priority];
    });
    setTodos(reorderedTodos);
  };

  const dragControls = useDragControls();

  const renderTodoCard = (todo, source) => (
    <TodoCard
      todo={todo}
      onComplete={source === 'active' ? handleComplete : undefined}
      onDelete={source === 'active' ? handleDelete : undefined}
      onRestore={source !== 'active' ? () => handleRestore(todo.id, source) : undefined}
      onPriorityChange={handlePriorityChange}
      isDragging={false}
      onSelect={() => setSelectedTodo(todo)}
      isCompleted={source === 'Completed'}
      isRemoved={source === 'Removed'}
      timestamp={source === 'Completed' ? todo.completedAt : source === 'Removed' ? todo.removedAt : undefined}
      dragControls={source === 'active' ? dragControls : undefined}
    />
  );

  return (
    <div style={{
      height: '100%',
      display: 'flex',
      flexDirection: isMobile ? 'column' : 'row',
      fontFamily: 'Poppins, sans-serif',
    }}>
      {!isMobile && (
        <ListSelector
          selectedList={selectedList}
          onSelectList={setSelectedList}
          lists={lists}
          counts={counts}
        />
      )}

      <div style={{
        flex: 1,
        padding: '1rem',
        maxWidth: isMobile ? '100%' : '800px',
        margin: '0 auto',
        display: 'flex',
        flexDirection: 'column',
        overflow: 'visible',
      }}>
        {isMobile && (
          <ListSelector
            selectedList={selectedList}
            onSelectList={setSelectedList}
            lists={lists}
            counts={counts}
          />
        )}

        {selectedList !== 'Completed' && selectedList !== 'Removed' && (
          <motion.button
            onClick={handleReorderWithAI}
            style={{
              position: 'sticky',
              top: '1rem',
              backgroundColor: '#528F75',
              color: 'white',
              border: 'none',
              borderRadius: '8px',
              padding: '0.75rem',
              marginBottom: '1rem',
              cursor: 'pointer',
              display: 'flex',
              alignItems: 'center',
              justifyContent: 'center',
              gap: '0.5rem',
              fontFamily: 'Poppins, sans-serif',
              zIndex: 10,
            }}
            whileHover={{ scale: 1.02 }}
            whileTap={{ scale: 0.98 }}
          >
            <FiRefreshCw />
            Reorder with AI
          </motion.button>
        )}

        <div style={{
          flex: 1,
          overflow: 'auto',
        }}>
          {selectedList === 'Completed' || selectedList === 'Removed' ? (
            <div style={{ padding: '0.5rem' }}>
              {filteredTodos.map((todo) => (
                <div key={todo.id}>
                  {renderTodoCard(todo, selectedList)}
                </div>
              ))}
            </div>
          ) : (
            <Reorder.Group
              axis="y"
              values={filteredTodos}
              onReorder={setTodos}
              style={{
                padding: '0.5rem',
                listStyle: 'none',
                margin: 0,
              }}
            >
              {filteredTodos.map((todo) => {
                const dragControls = useDragControls();
                return (
                <Reorder.Item
                  key={todo.id}
                  value={todo}
                  dragListener={false}
                  dragControls={dragControls}
                  style={{
                    listStyle: 'none',
                    margin: 0,
                    padding: 0,
                    width: '100%',
                    position: 'relative',
                    userSelect: 'none',
                    WebkitUserSelect: 'none',
                    MozUserSelect: 'none',
                    msUserSelect: 'none',
                  }}
                >
                  <TodoCard
      todo={todo}
      onComplete={handleComplete}
      onDelete={handleDelete}
      onRestore={() => handleRestore(todo.id, "active")}
      onPriorityChange={handlePriorityChange}
      isDragging={false}
      onSelect={() => setSelectedTodo(todo)}
      dragControls={dragControls}
                  />
                </Reorder.Item>
              );
              })}
            </Reorder.Group>
          )}
        </div>

        <AnimatePresence>
          {selectedTodo && (
            <TaskDetailModal
              task={selectedTodo}
              onClose={() => setSelectedTodo(null)}
              onComplete={handleComplete}
              onDelete={handleDelete}
              onPriorityChange={handlePriorityChange}
            />
          )}

          {showToast && (
            <Toast
              message={`Task ${lastAction === 'delete' ? 'deleted' : 'completed'}`}
              onUndo={handleRestore}
              onClose={() => setShowToast(false)}
              action={lastAction}
            />
          )}
        </AnimatePresence>
      </div>
    </div>
  );
}

export default TodoPage; 