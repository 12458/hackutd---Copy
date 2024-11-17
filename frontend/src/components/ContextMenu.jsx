import { motion } from 'framer-motion';
import PropTypes from 'prop-types';
import { FiTrash2, FiFlag } from 'react-icons/fi';

function ContextMenu({ x, y, onClose, onDelete, onPriorityChange, currentPriority }) {
  const priorities = ['High', 'Medium', 'Low'];

  return (
    <motion.div
      initial={{ opacity: 0, scale: 0.95 }}
      animate={{ opacity: 1, scale: 1 }}
      exit={{ opacity: 0, scale: 0.95 }}
      style={{
        position: 'fixed',
        top: y,
        left: x,
        backgroundColor: 'white',
        borderRadius: '8px',
        boxShadow: '0 2px 10px rgba(0,0,0,0.1)',
        padding: '0.5rem',
        zIndex: 1000,
        minWidth: '150px',
      }}
    >
      <div style={{
        display: 'flex',
        flexDirection: 'column',
        gap: '0.25rem',
      }}>
        <div style={{ 
          padding: '0.5rem', 
          borderBottom: '1px solid #eee',
          color: '#666',
          fontSize: '0.8rem',
          fontWeight: '500',
        }}>
          Priority
        </div>
        {priorities.map((priority) => (
          <button
            key={priority}
            onClick={() => {
              onPriorityChange(priority);
              onClose();
            }}
            style={{
              padding: '0.5rem',
              display: 'flex',
              alignItems: 'center',
              gap: '0.5rem',
              backgroundColor: priority === currentPriority ? '#f0f0f0' : 'transparent',
              border: 'none',
              borderRadius: '4px',
              cursor: 'pointer',
              width: '100%',
              textAlign: 'left',
              color: '#333',
              fontSize: '0.9rem',
            }}
          >
            <FiFlag style={{ color: priority === 'High' ? '#ef4444' : priority === 'Medium' ? '#f59e0b' : '#10b981' }} />
            {priority}
          </button>
        ))}
        <div style={{ margin: '0.25rem 0', borderTop: '1px solid #eee' }} />
        <button
          onClick={() => {
            onDelete();
            onClose();
          }}
          style={{
            padding: '0.5rem',
            display: 'flex',
            alignItems: 'center',
            gap: '0.5rem',
            backgroundColor: 'transparent',
            border: 'none',
            borderRadius: '4px',
            cursor: 'pointer',
            width: '100%',
            textAlign: 'left',
            color: '#ef4444',
            fontSize: '0.9rem',
          }}
        >
          <FiTrash2 />
          Delete Task
        </button>
      </div>
    </motion.div>
  );
}

ContextMenu.propTypes = {
  x: PropTypes.number.isRequired,
  y: PropTypes.number.isRequired,
  onClose: PropTypes.func.isRequired,
  onDelete: PropTypes.func.isRequired,
  onPriorityChange: PropTypes.func.isRequired,
  currentPriority: PropTypes.string.isRequired,
};

export default ContextMenu; 