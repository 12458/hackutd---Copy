import { motion } from 'framer-motion';
import PropTypes from 'prop-types';
import useIsMobile from '../hooks/useIsMobile';
import { MdDashboard, MdChecklist, MdPlayCircle } from 'react-icons/md';
import { useNavigate, useLocation } from 'react-router-dom';

function NavigationButtons({ style }) {
  const isMobile = useIsMobile();
  const location = useLocation();
  const navItems = [
    { text: "Dashboard", path: "/", icon: <MdDashboard /> },
    { text: "Todos", path: "/todos", icon: <MdChecklist /> },
    { text: "Actions", path: "/actions", icon: <MdPlayCircle /> },
  ];
  const navigate = useNavigate();

  return (
    <motion.nav 
      initial={isMobile ? { y: 100 } : { y: -100 }}
      animate={{ y: 0 }}
      transition={{ type: "spring", stiffness: 300, damping: 30 }}
    >
      <div style={{
        maxWidth: '500px',
        display: 'flex',
        gap: '0.5rem',
        margin: '0 auto',
        padding: '.5rem',
        backgroundColor: '#528F75',
        borderRadius: '6px 6px 0 0',
        flexDirection: 'row',
        alignItems: 'center',
        justifyContent: 'center',
      }}>
        {navItems.map((item, index) => {
          const isActive = location.pathname === item.path;
          return (
            <motion.button
              key={index}
              style={{
                padding: '0.5rem',
                backgroundColor: isActive ? '#fff' : '#528F75',
                color: isActive ? '#528F75' : '#fff',
                border: 'none',
                borderRadius: '6px',
                cursor: 'pointer',
                flex: 1,
                fontSize: '0.9rem',
                fontFamily: 'Poppins, sans-serif',
                fontWeight: '500',
                display: 'flex',
                flexDirection: 'row',
                alignItems: 'center',
                justifyContent: 'center',
                gap: '0.25rem'
              }}
              onClick={() => navigate(item.path)}
              whileHover={{ scale: 1.05 }}
              whileTap={{ scale: 0.95 }}
            >
              {item.icon}
              {item.text}
            </motion.button>
          );
        })}
      </div>
    </motion.nav>
  );
}

NavigationButtons.propTypes = {
  style: PropTypes.object,
};

export default NavigationButtons;