import { motion, AnimatePresence } from 'framer-motion';
import PropTypes from 'prop-types';
import { FiList, FiChevronDown } from 'react-icons/fi';
import { useState, useRef, useEffect } from 'react';
import useIsMobile from '../hooks/useIsMobile';

function ListSelector({ selectedList, onSelectList, lists, counts }) {
  const [isOpen, setIsOpen] = useState(false);
  const isMobile = useIsMobile();
  const dropdownRef = useRef(null);

  useEffect(() => {
    const handleClickOutside = (event) => {
      if (dropdownRef.current && !dropdownRef.current.contains(event.target)) {
        setIsOpen(false);
      }
    };

    document.addEventListener('mousedown', handleClickOutside);
    return () => document.removeEventListener('mousedown', handleClickOutside);
  }, []);

  const getListIcon = (list) => {
    switch(list) {
      case 'HVAC': return '🌡️';
      case 'Water': return '💧';
      case 'Gas': return '🔥';
      case 'Elevator': return '🛗';
      case 'Report': return '📋';
      default: return '📝';
    }
  };

  if (isMobile) {
    return (
      <div style={{
        position: 'relative',
        zIndex: 50,
      }} ref={dropdownRef}>
        <motion.button
          onClick={() => setIsOpen(!isOpen)}
          style={{
            width: '100%',
            padding: '0.75rem 1rem',
            backgroundColor: '#fff',
            border: '1px solid #eee',
            borderRadius: '8px',
            display: 'flex',
            alignItems: 'center',
            justifyContent: 'space-between',
            cursor: 'pointer',
            fontFamily: 'Poppins, sans-serif',
            color: '#333',
            marginBottom: isOpen ? '0' : '1rem',
          }}
        >
          <div style={{ display: 'flex', alignItems: 'center', gap: '0.5rem' }}>
            <span>{getListIcon(selectedList)}</span>
            <span>{selectedList === 'all' ? 'All Lists' : selectedList}</span>
            <span style={{ color: '#666', fontSize: '0.9rem' }}>
              ({counts[selectedList] || 0})
            </span>
          </div>
          <FiChevronDown style={{
            transform: isOpen ? 'rotate(180deg)' : 'rotate(0deg)',
            transition: 'transform 0.2s ease',
          }} />
        </motion.button>

        <AnimatePresence>
          {isOpen && (
            <motion.div
              initial={{ opacity: 0, y: -10 }}
              animate={{ opacity: 1, y: 0 }}
              exit={{ opacity: 0, y: -10 }}
              style={{
                position: 'absolute',
                top: '100%',
                left: 0,
                right: 0,
                backgroundColor: '#fff',
                borderRadius: '0 0 8px 8px',
                boxShadow: '0 4px 6px rgba(0,0,0,0.1)',
                marginBottom: '1rem',
                overflow: 'hidden',
              }}
            >
              {['all', ...lists].map((list) => (
                <motion.button
                  key={list}
                  onClick={() => {
                    onSelectList(list);
                    setIsOpen(false);
                  }}
                  whileHover={{ backgroundColor: '#f5f5f5' }}
                  style={{
                    width: '100%',
                    padding: '0.75rem 1rem',
                    backgroundColor: selectedList === list ? '#f0f0f0' : 'transparent',
                    border: 'none',
                    display: 'flex',
                    alignItems: 'center',
                    gap: '0.5rem',
                    cursor: 'pointer',
                    fontFamily: 'Poppins, sans-serif',
                    color: '#333',
                    borderTop: '1px solid #eee',
                  }}
                >
                  <span>{getListIcon(list)}</span>
                  <span>{list === 'all' ? 'All Lists' : list}</span>
                  <span style={{ 
                    color: '#666', 
                    fontSize: '0.9rem', 
                    marginLeft: 'auto',
                    backgroundColor: '#f0f0f0',
                    padding: '0.25rem 0.5rem',
                    borderRadius: '12px',
                  }}>
                    {counts[list] || 0}
                  </span>
                </motion.button>
              ))}
            </motion.div>
          )}
        </AnimatePresence>
      </div>
    );
  }

  return (
    <div style={{
      width: '250px',
      padding: '1rem',
      borderRight: '1px solid #eee',
      height: '100%',
      backgroundColor: '#fff',
      fontFamily: 'Poppins, sans-serif',
    }}>
      <h2 style={{ 
        fontSize: '1.1rem', 
        marginBottom: '1rem',
        color: '#333',
        display: 'flex',
        alignItems: 'center',
        gap: '0.5rem',
      }}>
        <FiList />
        Lists
      </h2>
      <div style={{ display: 'flex', flexDirection: 'column', gap: '0.5rem' }}>
        {['all', ...lists].map((list) => (
          <motion.button
            key={list}
            onClick={() => onSelectList(list)}
            whileHover={{ backgroundColor: '#f5f5f5' }}
            style={{
              padding: '0.75rem 1rem',
              backgroundColor: selectedList === list ? '#f0f0f0' : 'transparent',
              border: 'none',
              borderRadius: '8px',
              display: 'flex',
              alignItems: 'center',
              gap: '0.5rem',
              cursor: 'pointer',
              fontFamily: 'Poppins, sans-serif',
              color: '#333',
              textAlign: 'left',
            }}
          >
            <span>{getListIcon(list)}</span>
            <span>{list === 'all' ? 'All Lists' : list}</span>
            <span style={{ 
              color: '#666', 
              fontSize: '0.9rem', 
              marginLeft: 'auto',
              backgroundColor: '#f0f0f0',
              padding: '0.25rem 0.5rem',
              borderRadius: '12px',
            }}>
              {counts[list] || 0}
            </span>
          </motion.button>
        ))}
      </div>
    </div>
  );
}

ListSelector.propTypes = {
  selectedList: PropTypes.string.isRequired,
  onSelectList: PropTypes.func.isRequired,
  lists: PropTypes.arrayOf(PropTypes.string).isRequired,
  counts: PropTypes.object.isRequired,
};

export default ListSelector; 