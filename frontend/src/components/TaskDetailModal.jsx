import { motion } from 'framer-motion';
import PropTypes from 'prop-types';
import { FiX, FiCheck, FiTrash2, FiFlag } from 'react-icons/fi';
import ReactMarkdown from 'react-markdown';

function TaskDetailModal({ task, onClose, onComplete, onDelete, onPriorityChange }) {
  const priorities = ['High', 'Medium', 'Low'];

  return (
    <motion.div
      initial={{ opacity: 0 }}
      animate={{ opacity: 1 }}
      exit={{ opacity: 0 }}
      style={{
        position: 'fixed',
        top: 0,
        left: 0,
        right: 0,
        bottom: 0,
        backgroundColor: 'rgba(0,0,0,0.5)',
        display: 'flex',
        alignItems: 'center',
        justifyContent: 'center',
        zIndex: 1000,
        padding: '1rem',
        fontFamily: 'Poppins, sans-serif',
      }}
      onClick={onClose}
    >
      <motion.div
        initial={{ scale: 0.95 }}
        animate={{ scale: 1 }}
        style={{
          backgroundColor: 'white',
          borderRadius: '8px',
          width: '100%',
          maxWidth: '600px',
          maxHeight: '90vh',
          overflow: 'auto',
          padding: '1.5rem',
          fontFamily: 'Poppins, sans-serif',
        }}
        onClick={(e) => e.stopPropagation()}
      >
        <div style={{
          display: 'flex',
          justifyContent: 'space-between',
          alignItems: 'flex-start',
          marginBottom: '1.5rem',
        }}>
          <h2 style={{ 
            fontSize: '1.5rem',
            fontWeight: '600',
            color: '#333',
          }}>{task.title}</h2>
          <button
            onClick={onClose}
            style={{
              backgroundColor: 'transparent',
              border: 'none',
              cursor: 'pointer',
              padding: '0.5rem',
              borderRadius: '50%',
            }}
          >
            <FiX size={24} />
          </button>
        </div>

        <div style={{
          display: 'flex',
          gap: '1rem',
          marginBottom: '1rem',
        }}>
          <select
            value={task.priority}
            onChange={(e) => onPriorityChange(e.target.value)}
            style={{
              padding: '0.5rem',
              borderRadius: '4px',
              border: '1px solid #ddd',
              backgroundColor: 'white',
              cursor: 'pointer',
            }}
          >
            {priorities.map((priority) => (
              <option key={priority} value={priority}>
                {priority} Priority
              </option>
            ))}
          </select>

          <div style={{ flex: 1 }} />

          <motion.button
            whileHover={{ scale: 1.05 }}
            whileTap={{ scale: 0.95 }}
            onClick={onComplete}
            style={{
              backgroundColor: '#528F75',
              color: 'white',
              border: 'none',
              borderRadius: '4px',
              padding: '0.5rem 1rem',
              display: 'flex',
              alignItems: 'center',
              gap: '0.5rem',
              cursor: 'pointer',
            }}
          >
            <FiCheck />
            Complete
          </motion.button>

          <motion.button
            whileHover={{ scale: 1.05 }}
            whileTap={{ scale: 0.95 }}
            onClick={onDelete}
            style={{
              backgroundColor: '#fee2e2',
              color: '#ef4444',
              border: 'none',
              borderRadius: '4px',
              padding: '0.5rem 1rem',
              display: 'flex',
              alignItems: 'center',
              gap: '0.5rem',
              cursor: 'pointer',
            }}
          >
            <FiTrash2 />
            Delete
          </motion.button>
        </div>

        <div style={{
          backgroundColor: '#f8f9fa',
          padding: '1rem',
          borderRadius: '8px',
          marginTop: '1rem',
          fontFamily: 'Poppins, sans-serif',
        }}>
          <ReactMarkdown
            components={{
              h1: ({ children }) => (
                <h1 style={{ 
                  fontFamily: 'Poppins, sans-serif',
                  fontSize: '1.5rem',
                  fontWeight: '600',
                  marginBottom: '1rem',
                  color: '#333',
                }}>{children}</h1>
              ),
              p: ({ children }) => (
                <p style={{ 
                  fontFamily: 'Poppins, sans-serif',
                  marginBottom: '0.5rem',
                  color: '#666',
                }}>{children}</p>
              ),
              strong: ({ children }) => (
                <strong style={{ 
                  fontFamily: 'Poppins, sans-serif',
                  fontWeight: '600',
                  color: '#333',
                }}>{children}</strong>
              ),
              li: ({ children }) => (
                <li style={{ 
                  fontFamily: 'Poppins, sans-serif',
                  marginBottom: '0.25rem',
                  color: '#666',
                }}>{children}</li>
              ),
            }}
          >
            {task.description || '*No description provided*'}
          </ReactMarkdown>
        </div>
      </motion.div>
    </motion.div>
  );
}

TaskDetailModal.propTypes = {
  task: PropTypes.shape({
    id: PropTypes.string.isRequired,
    title: PropTypes.string.isRequired,
    description: PropTypes.string,
    priority: PropTypes.string.isRequired,
  }).isRequired,
  onClose: PropTypes.func.isRequired,
  onComplete: PropTypes.func.isRequired,
  onDelete: PropTypes.func.isRequired,
  onPriorityChange: PropTypes.func.isRequired,
};

export default TaskDetailModal; 