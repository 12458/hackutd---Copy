import { Routes, Route } from 'react-router-dom';
import useIsMobile from './hooks/useIsMobile';
import NavigationButtons from './components/NavigationButtons';
import { motion } from 'framer-motion';
import { AuthProvider } from './context/AuthContext';
import UserMenu from './components/UserMenu';
import DashboardPage from './pages/DashboardPage';
import TodoPage from './pages/TodoPage';
import ActionsPage from './pages/ActionsPage';

function App() {
  const isMobile = useIsMobile();


  return (
    <AuthProvider>
      <div style={{
        height: '100vh',
        width: '100vw',
        display: 'flex',
        flexDirection: 'column',
        backgroundColor: '#F5F5F5',
        overflow: 'hidden',
        position: 'relative',
        margin: '0',
        padding: '0',
      }}>
        <div style={{
          backgroundColor: '#528F75',
          padding: '0.75rem',
          display: 'flex',
          justifyContent: 'space-between',
          alignItems: 'center',
          color: 'white',
          borderBottomLeftRadius: '8px',
          borderBottomRightRadius: '8px',
          margin: '0',
        }}>
          <div style={{
            display: 'flex',
            alignItems: 'center',
            gap: '0.5rem',
            fontSize: '0.9rem',
            fontFamily: 'Poppins, sans-serif',
          }}>
            <motion.div 
              style={{
                backgroundColor: '#fff',
                padding: '0.5rem 0.75rem',
                borderRadius: '6px',
                display: 'flex',
                alignItems: 'center',
                direction: 'column',
                gap: '0.5rem',
              color: '#528F75',
              boxShadow: '0px 2px 4px rgba(0, 0, 0, 0.1)',
              cursor: 'pointer',
            }}
            whileHover={{
              scale: 1.05,
            }}
            whileTap={{
              scale: 0.95,
            }}
            >
              <div>
              <span>🏢</span>
              <span style={{
                fontWeight: '600',
              }}>Acme Buildings</span>
              </div>

            </motion.div>
          </div>
          {!isMobile && <NavigationButtons  />}
          <UserMenu />
        </div>

        <main style={{
          flex: 1,
          backgroundColor: '#F5F5F5',
          overflow: 'auto',
          paddingBottom: isMobile ? '80px' : '0',
        }}>
          <Routes>
            <Route path="/" element={<DashboardPage />} />
            <Route path="/todos" element={<TodoPage />} />
            <Route path="/actions" element={<ActionsPage />} />
          </Routes>
        </main>

        {isMobile && <NavigationButtons style={{
          backgroundColor: 'transparent',
          padding: '1rem',
          position: 'fixed',
          bottom: '0',
          width: '100%',
          display: 'flex',
          justifyContent: 'space-around',
          gap: '0.5rem',
        }} />}
      </div>
    </AuthProvider>
  );
}

export default App;
