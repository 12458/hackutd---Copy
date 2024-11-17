import { motion, useMotionValue, useTransform } from 'framer-motion';
import PropTypes from 'prop-types';
import { FiCheck, FiClock, FiTag, FiMenu, FiTrash2, FiRefreshCw, FiMoreVertical } from 'react-icons/fi';
import { useState } from 'react';
import ContextMenu from './ContextMenu';
import useIsMobile from '../hooks/useIsMobile';

function TodoCard({ 
  todo, 
  onComplete, 
  onDelete, 
  onRestore,
  onPriorityChange, 
  isDragging, 
  onSelect,
  isCompleted,
  isRemoved,
  timestamp,
  dragControls
}) {
  const [showContextMenu, setShowContextMenu] = useState(false);
  const [contextMenuPosition, setContextMenuPosition] = useState({ x: 0, y: 0 });
  const [isDraggingHandle, setIsDraggingHandle] = useState(false);
  const isMobile = useIsMobile();
  
  const x = useMotionValue(0);
  const background = useTransform(
    x,
    [-200, -100, 0, 100, 200],
    ['#4ADE80', '#4ADE8080', '#ffffff', '#ff444480', '#ff4444']
  );
  const opacity = useTransform(
    x, 
    [-200, -100, 0, 100, 200], 
    [0.8, 0.9, 1, 0.9, 0.8]
  );

  const completeScale = useTransform(
    x,
    [-200, -100, 0],
    [1.2, 1, 0]
  );

  const deleteScale = useTransform(
    x,
    [0, 100, 200],
    [0, 1, 1.2]
  );

  const handleContextMenu = (e) => {
    e.preventDefault();
    setContextMenuPosition({ x: e.clientX, y: e.clientY });
    setShowContextMenu(true);
  };

  const buttonSize = isMobile ? '44px' : '32px';
  const iconSize = isMobile ? 20 : 16;

  const formattedTimestamp = timestamp ? new Date(timestamp).toLocaleString() : null;

  const handleDragEnd = () => {
    if (!isDraggingHandle) {
      const xValue = x.get();
      if (xValue <= -100 && onComplete) {
        onComplete(todo.id);
      } else if (xValue >= 100 && onDelete) {
        onDelete(todo.id);
      }
    }
    x.set(0);
  };

  return (
    <>
      <motion.div
        style={{
          x,
          background,
          opacity,
          width: '100%',
          padding: isMobile ? '1rem 0.75rem' : '1rem',
          borderRadius: '8px',
          boxShadow: '0 2px 4px rgba(0,0,0,0.1)',
          cursor: isDragging ? 'grabbing' : 'grab',
          touchAction: 'none',
          backgroundColor: isCompleted ? '#f0fdf4' : isRemoved ? '#fef2f2' : '#fff',
          userSelect: 'none',
          WebkitUserSelect: 'none',
          MozUserSelect: 'none',
          msUserSelect: 'none',
          position: 'relative',
          marginBottom: '1rem',
        }}
        whileHover={{ scale: 1.02 }}
        onContextMenu={handleContextMenu}
        onClick={onSelect}
        drag={!isCompleted && !isRemoved && !isDraggingHandle ? "x" : undefined}
        dragConstraints={{ left: -200, right: 200 }}
        dragElastic={0.3}
        dragMomentum={false}
        dragTransition={{ 
          power: 0.3,
          timeConstant: 200,
          min: -200,
          max: 200
        }}
        onDragEnd={handleDragEnd}
      >
        <motion.div
          style={{
            position: 'absolute',
            left: 20,
            top: '50%',
            translateY: '-50%',
            scale: completeScale,
            color: '#4ADE80',
            zIndex: 0,
            pointerEvents: 'none',
          }}
        >
          <FiCheck size={24} />
        </motion.div>
        <motion.div
          style={{
            position: 'absolute',
            right: 20,
            top: '50%',
            translateY: '-50%',
            scale: deleteScale,
            color: '#ff4444',
            zIndex: 0,
            pointerEvents: 'none',
          }}
        >
          <FiTrash2 size={24} />
        </motion.div>

        <div style={{
          display: 'flex',
          justifyContent: 'space-between',
          alignItems: 'center',
          gap: '0.5rem',
          pointerEvents: 'none',
        }}>
          <div style={{ display: 'flex', alignItems: 'center', gap: '0.75rem', flex: 1 }}>
            {!isCompleted && !isRemoved && (
              <div
                onPointerDown={(e) => {
                  setIsDraggingHandle(true);
                  dragControls?.start(e);
                }}
                onPointerUp={() => {
                  setIsDraggingHandle(false);
                }}
                style={{ 
                  color: '#666', 
                  cursor: 'grab',
                  touchAction: 'none',
                  pointerEvents: 'auto',
                }}
              >
                <FiMenu />
              </div>
            )}
            <div style={{ flex: 1, minWidth: 0 }}>
              <h3 style={{ 
                fontSize: '1rem', 
                marginBottom: '0.5rem',
                fontFamily: 'Poppins, sans-serif',
                color: '#333',
                whiteSpace: 'nowrap',
                overflow: 'hidden',
                textOverflow: 'ellipsis',
                textDecoration: isCompleted ? 'line-through' : 'none',
              }}>{todo.title}</h3>
              <div style={{ 
                display: 'flex', 
                flexDirection: 'column',
                gap: '0.25rem', 
                fontSize: '0.8rem',
                color: '#666'
              }}>
                <div style={{ display: 'flex', gap: '1rem' }}>
                  <span style={{ display: 'flex', alignItems: 'center', gap: '0.25rem' }}>
                    <FiClock /> {todo.dueDate}
                  </span>
                  <span style={{ display: 'flex', alignItems: 'center', gap: '0.25rem' }}>
                    <FiTag /> {todo.priority}
                  </span>
                </div>
                {timestamp && (
                  <span style={{ fontSize: '0.75rem', color: '#888' }}>
                    {isCompleted ? 'Completed' : 'Removed'} at: {formattedTimestamp}
                  </span>
                )}
              </div>
            </div>
          </div>
          <div style={{ 
            display: 'flex', 
            gap: '0.75rem',
            marginLeft: 'auto',
            padding: isMobile ? '0.25rem' : 0,
            pointerEvents: 'auto',
          }}>
            {onRestore ? (
              <motion.button
                whileHover={{ scale: 1.1 }}
                whileTap={{ scale: 0.9 }}
                onClick={(e) => {
                  e.stopPropagation();
                  onRestore();
                }}
                style={{
                  backgroundColor: '#528F75',
                  color: 'white',
                  border: 'none',
                  borderRadius: '50%',
                  width: buttonSize,
                  height: buttonSize,
                  display: 'flex',
                  alignItems: 'center',
                  justifyContent: 'center',
                  cursor: 'pointer',
                  padding: 0,
                }}
              >
                <FiRefreshCw size={iconSize} />
              </motion.button>
            ) : (
              <>
                <motion.button
                  whileHover={{ scale: 1.1 }}
                  whileTap={{ scale: 0.9 }}
                  onClick={(e) => {
                    e.stopPropagation();
                    onDelete(todo.id);
                  }}
                  style={{
                    backgroundColor: '#fee2e2',
                    color: '#ef4444',
                    border: 'none',
                    borderRadius: '50%',
                    width: buttonSize,
                    height: buttonSize,
                    display: 'flex',
                    alignItems: 'center',
                    justifyContent: 'center',
                    cursor: 'pointer',
                    padding: 0,
                  }}
                >
                  <FiTrash2 size={iconSize} />
                </motion.button>
                <motion.button
                  whileHover={{ scale: 1.1 }}
                  whileTap={{ scale: 0.9 }}
                  onClick={(e) => {
                    e.stopPropagation();
                    onComplete(todo.id);
                  }}
                  style={{
                    backgroundColor: '#528F75',
                    color: 'white',
                    border: 'none',
                    borderRadius: '50%',
                    width: buttonSize,
                    height: buttonSize,
                    display: 'flex',
                    alignItems: 'center',
                    justifyContent: 'center',
                    cursor: 'pointer',
                    padding: 0,
                  }}
                >
                  <FiCheck size={iconSize} />
                </motion.button>
              </>
            )}
          </div>
        </div>
      </motion.div>

      {showContextMenu && (
        <ContextMenu
          x={contextMenuPosition.x}
          y={contextMenuPosition.y}
          onClose={() => setShowContextMenu(false)}
          onDelete={() => onDelete(todo.id)}
          onPriorityChange={(priority) => onPriorityChange(todo.id, priority)}
          currentPriority={todo.priority}
        />
      )}
    </>
  );
}

TodoCard.propTypes = {
  todo: PropTypes.shape({
    id: PropTypes.string.isRequired,
    title: PropTypes.string.isRequired,
    dueDate: PropTypes.string.isRequired,
    priority: PropTypes.string.isRequired,
    description: PropTypes.string,
  }).isRequired,
  onComplete: PropTypes.func,
  onDelete: PropTypes.func,
  onRestore: PropTypes.func,
  onPriorityChange: PropTypes.func.isRequired,
  isDragging: PropTypes.bool.isRequired,
  onSelect: PropTypes.func.isRequired,
  isCompleted: PropTypes.bool,
  isRemoved: PropTypes.bool,
  timestamp: PropTypes.string,
  dragControls: PropTypes.object,
};

export default TodoCard; 