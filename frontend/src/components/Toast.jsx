import { motion, AnimatePresence } from 'framer-motion';
import PropTypes from 'prop-types';
import { FiX, FiRotateCcw } from 'react-icons/fi';
import useIsMobile from '../hooks/useIsMobile';

function Toast({ message, onUndo, onClose, action }) {
  const isMobile = useIsMobile();

  return (
    <AnimatePresence>
      <motion.div
        initial={{ opacity: 0, x: 100, y: 0 }}
        animate={{ opacity: 1, x: 0, y: 0 }}
        exit={{ opacity: 0, x: 100, y: 0 }}
        style={{
          position: 'fixed',
          bottom: isMobile ? '80px' : '20px',
          right: '20px',
          backgroundColor: '#333',
          color: 'white',
          padding: '0.75rem 1rem',
          borderRadius: '8px',
          display: 'flex',
          alignItems: 'center',
          gap: '1rem',
          boxShadow: '0 4px 6px rgba(0, 0, 0, 0.1)',
          zIndex: 1000,
          fontFamily: 'Poppins, sans-serif',
          maxWidth: isMobile ? 'calc(100% - 40px)' : '400px',
          width: 'auto',
        }}
      >
        <span style={{ fontSize: '0.9rem' }}>{message}</span>
        <div style={{ display: 'flex', gap: '0.5rem', alignItems: 'center', flexShrink: 0 }}>
          <motion.button
            whileHover={{ scale: 1.05 }}
            whileTap={{ scale: 0.95 }}
            onClick={() => {
              onUndo();
              onClose();
            }}
            style={{
              backgroundColor: '#528F75',
              border: 'none',
              padding: '0.5rem',
              borderRadius: '4px',
              cursor: 'pointer',
              display: 'flex',
              alignItems: 'center',
              gap: '0.25rem',
              color: 'white',
              fontSize: '0.8rem',
              fontFamily: 'Poppins, sans-serif',
              whiteSpace: 'nowrap',
            }}
          >
            <FiRotateCcw size={14} />
            Undo {action}
          </motion.button>
          <motion.button
            whileHover={{ scale: 1.05 }}
            whileTap={{ scale: 0.95 }}
            onClick={onClose}
            style={{
              backgroundColor: 'transparent',
              border: 'none',
              padding: '0.25rem',
              cursor: 'pointer',
              color: '#999',
              display: 'flex',
              alignItems: 'center',
              flexShrink: 0,
            }}
          >
            <FiX size={18} />
          </motion.button>
        </div>
      </motion.div>
    </AnimatePresence>
  );
}

Toast.propTypes = {
  message: PropTypes.string.isRequired,
  onUndo: PropTypes.func.isRequired,
  onClose: PropTypes.func.isRequired,
  action: PropTypes.string.isRequired,
};

export default Toast; 