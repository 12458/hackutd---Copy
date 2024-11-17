import { useState, useEffect, useRef } from 'react';
import { motion, AnimatePresence } from 'framer-motion';
import { auth, googleProvider } from '../firebase';
import { 
  signInWithPopup, 
  signInWithEmailAndPassword, 
  createUserWithEmailAndPassword, 
  signOut 
} from 'firebase/auth';
import { useAuth } from '../context/AuthContext';
import { FiLogIn, FiUserPlus, FiLogOut, FiAlertCircle } from 'react-icons/fi';
import { FcGoogle } from 'react-icons/fc';
import structLogo from '../assets/struct-logo.svg';
import QRCode from "react-qr-code";


function UserMenu() {
  const [isOpen, setIsOpen] = useState(false);
  const [showAuthModal, setShowAuthModal] = useState(false);
  const [isSignUp, setIsSignUp] = useState(false);
  const [email, setEmail] = useState('');
  const [password, setPassword] = useState('');
  const [error, setError] = useState('');
  const { user } = useAuth();
  const dropdownRef = useRef(null);

  // Handle clicking outside of the dropdown
  useEffect(() => {
    const handleClickOutside = (event) => {
      if (dropdownRef.current && !dropdownRef.current.contains(event.target)) {
        setIsOpen(false);
      }
    };

    document.addEventListener('mousedown', handleClickOutside);
    return () => {
      document.removeEventListener('mousedown', handleClickOutside);
    };
  }, []);

  const handleAuthModalOpen = () => {
    setShowAuthModal(true);
    setIsOpen(false); // Close the profile popup when opening the modal
  };

  const handleGoogleSignIn = async () => {
    try {
      setError('');
      await signInWithPopup(auth, googleProvider);
      setShowAuthModal(false);
    } catch (error) {
      setError('Failed to sign in with Google. Please try again.');
    }
  };

  const handleEmailAuth = async (e) => {
    e.preventDefault();
    setError('');
    
    try {
      if (isSignUp) {
        if (password.length < 6) {
          setError('Password must be at least 6 characters long');
          return;
        }
        await createUserWithEmailAndPassword(auth, email, password);
      } else {
        await signInWithEmailAndPassword(auth, email, password);
      }
      setShowAuthModal(false);
      setEmail('');
      setPassword('');
    } catch (error) {
      switch (error.code) {
        case 'auth/user-not-found':
        case 'auth/wrong-password':
          setError('Incorrect email or password');
          break;
        case 'auth/email-already-in-use':
          setError('Email is already registered');
          break;
        case 'auth/invalid-email':
          setError('Invalid email address');
          break;
        case 'auth/weak-password':
          setError('Password is too weak');
          break;
        default:
          setError('Failed to sign in. Please try again.');
      }
    }
  };

  const handleSignOut = async () => {
    try {
      await signOut(auth);
      setIsOpen(false);
    } catch (error) {
      console.error('Error signing out:', error);
    }
  };

  return (
    <div style={{ position: 'relative' }} ref={dropdownRef}>
      <motion.div
        onClick={() => setIsOpen(!isOpen)}
        style={{
          width: '32px',
          height: '32px',
          backgroundColor: '#fff',
          borderRadius: '50%',
          display: 'flex',
          alignItems: 'center',
          justifyContent: 'center',
          fontSize: '1.2rem',
          padding: '0.5rem',
          cursor: 'pointer',
        }}
        whileHover={{ scale: 1.05 }}
        whileTap={{ scale: 0.95 }}
      >
        {user?.photoURL ? (
          <img 
            src={user.photoURL} 
            alt="User" 
            style={{ width: '100%', height: '100%', borderRadius: '50%' }}
          />
        ) : (
          '👤'
        )}
      </motion.div>

      <AnimatePresence>
        {isOpen && (
          <motion.div
            initial={{ opacity: 0, y: -10 }}
            animate={{ opacity: 1, y: 0 }}
            exit={{ opacity: 0, y: -10 }}
            style={{
              position: 'absolute',
              right: 0,
              top: '45px',
              backgroundColor: 'white',
              padding: '1rem',
              borderRadius: '8px',
              boxShadow: '0 2px 10px rgba(0,0,0,0.1)',
              width: '200px',
              zIndex: 1000,
            }}
          >
            {user ? (
              <>
                <div style={{ marginBottom: '1rem', zIndex: "10", position: "relative"}}>
                  <p style={{ fontWeight: '500', color: '#333', fontFamily: 'Poppins, sans-serif', position: "relative"}}>Welcome, {user.displayName || user.email}</p>
                  <p style={{ fontSize: '0.8rem', color: '#666', fontFamily: 'Poppins, sans-serif' }}>Building manager at ACME</p>
                  <p style={{ fontSize: '0.8rem', color: '#666', fontFamily: 'Poppins, sans-serif' }}>{user.email}</p>
                  <p style={{ fontSize: '0.6rem', color: '#666', fontFamily: 'Poppins, sans-serif' }}>Last signed in at: {user.metadata.lastSignInTime}</p>
                  <p style={{ fontSize: '0.6rem', color: '#666', fontFamily: 'Poppins, sans-serif' }}>ID: {user.uid}</p>
                  <QRCode value={user.uid} style={{ width: '80%', height: 'auto', marginTop: '1rem', marginBottom: '1rem', marginLeft: 'auto', marginRight: 'auto', display: 'block' }} />
                </div>
                <button
                  onClick={handleSignOut}
                  style={{
                    width: '100%',
                    padding: '0.5rem',
                    backgroundColor: '#528F75',
                    color: 'white',
                    border: 'none',
                    borderRadius: '4px',
                    cursor: 'pointer',
                    fontFamily: 'Poppins, sans-serif',
                    display: 'flex',
                    alignItems: 'center',
                    justifyContent: 'center',
                    gap: '0.5rem',
                  }}
                >
                  <FiLogOut />
                  Sign Out
                </button>
              </>
            ) : (
              <button
                onClick={handleAuthModalOpen}
                style={{
                  width: '100%',
                  padding: '0.5rem',
                  backgroundColor: '#528F75',
                  color: 'white',
                  border: 'none',
                  borderRadius: '4px',
                  cursor: 'pointer',
                  fontFamily: 'Poppins, sans-serif',
                  display: 'flex',
                  alignItems: 'center',
                  justifyContent: 'center',
                  gap: '0.5rem',
                }}
              >
                <FiLogIn />
                Sign In
              </button>
            )}
          </motion.div>
        )}

        {showAuthModal && (
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
            }}
            onClick={(e) => {e.stopPropagation(); setShowAuthModal(false); setIsOpen(false); setError('');}}
          >
            <motion.div
              initial={{ scale: 0.9 }}
              animate={{ scale: 1 }}
              style={{
                backgroundColor: 'white',
                padding: '2rem',
                borderRadius: '8px',
                width: '90%',
                maxWidth: '400px',
              }}
              onClick={(e) => e.stopPropagation()}
            >
              <div style={{
                display: 'flex',
                flexDirection: 'column',
                alignItems: 'center',
                gap: '1.5rem',
                marginBottom: '2rem',
              }}>
                <img 
                  src={structLogo} 
                  alt="Struct Logo" 
                  style={{
                    width: '120px',
                    height: 'auto',
                  }}
                />
              </div>

              {error && (
                <motion.div
                  initial={{ opacity: 0, y: -10 }}
                  animate={{ opacity: 1, y: 0 }}
                  style={{
                    padding: '0.75rem',
                    backgroundColor: '#FEE2E2',
                    border: '1px solid #FCA5A5',
                    borderRadius: '4px',
                    marginBottom: '1rem',
                    display: 'flex',
                    alignItems: 'center',
                    gap: '0.5rem',
                    color: '#DC2626',
                    fontSize: '0.9rem',
                  }}
                >
                  <FiAlertCircle />
                  {error}
                </motion.div>
              )}

              <form onSubmit={handleEmailAuth} style={{
                width: '100%', 
                display: 'flex', 
                flexDirection: 'column', 
                alignItems: 'center', 
                justifyContent: 'center', 
                gap: '1rem'
              }}>
                <input
                  type="email"
                  placeholder="Email"
                  value={email}
                  onChange={(e) => setEmail(e.target.value)}
                  style={{
                    padding: '0.5rem',
                    marginBottom: '1rem',
                    borderRadius: '4px',
                    border: '1px solid #ddd',
                    fontFamily: 'Poppins, sans-serif',
                    width: '100%',
                    boxSizing: 'border-box',
                  }}
                />
                <input
                  type="password"
                  placeholder="Password"
                  value={password}
                  onChange={(e) => setPassword(e.target.value)}
                  style={{
                    margin: 0,
                    padding: '0.5rem',
                    marginBottom: '1rem',
                    borderRadius: '4px',
                    border: '1px solid #ddd',
                    fontFamily: 'Poppins, sans-serif',
                    width: '100%',
                    boxSizing: 'border-box',
                  }}
                />
                <button
                  type="submit"
                  style={{
                    padding: '0.5rem',
                    backgroundColor: '#528F75',
                    color: 'white',
                    border: 'none',
                    borderRadius: '4px',
                    marginBottom: '1rem',
                    cursor: 'pointer',
                    fontFamily: 'Poppins, sans-serif',
                    display: 'flex',
                    alignItems: 'center',
                    justifyContent: 'center',
                    gap: '0.5rem',
                    width: '100%',
                  }}
                >
                  {isSignUp ? <FiUserPlus /> : <FiLogIn />}
                  {isSignUp ? 'Sign Up' : 'Sign In'}
                </button>
              </form>

              <button
                onClick={handleGoogleSignIn}
                style={{
                  width: '100%',
                  padding: '0.5rem',
                  backgroundColor: 'white',
                  border: '1px solid #ddd',
                  borderRadius: '4px',
                  marginBottom: '1rem',
                  cursor: 'pointer',
                  display: 'flex',
                  alignItems: 'center',
                  justifyContent: 'center',
                  gap: '0.5rem',
                  fontFamily: 'Poppins, sans-serif',
                }}
              >
                <FcGoogle size={20} />
                Sign in with Google
              </button>
              <button
                onClick={() => {
                  setIsSignUp(!isSignUp);
                  setError('');
                }}
                style={{
                  padding: '0.5rem',
                  backgroundColor: 'transparent',
                  border: 'none',
                  cursor: 'pointer',
                  color: '#528F75',
                  fontFamily: 'Poppins, sans-serif',
                  display: 'flex',
                  alignItems: 'center',
                  justifyContent: 'center',
                  gap: '0.5rem',
                  width: '100%',
                }}
              >
                {isSignUp ? <FiLogIn /> : <FiUserPlus />}
                {isSignUp ? 'Already have an account? Sign In' : 'Need an account? Sign Up'}
              </button>
            </motion.div>
          </motion.div>
        )}
      </AnimatePresence>
    </div>
  );
}

export default UserMenu; 