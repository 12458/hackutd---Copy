// Import the functions you need from the SDKs you need
import { initializeApp } from "firebase/app";
import { getFirestore } from "firebase/firestore";
import { getAuth, GoogleAuthProvider } from "firebase/auth";

// TODO: Add SDKs for Firebase products that you want to use
// https://firebase.google.com/docs/web/setup#available-libraries

// Your web app's Firebase configuration
const firebaseConfig = {
  apiKey: "AIzaSyCbZ15lV6zvy7JyLzKykboPJYjMaYOxp_A",
  authDomain: "hackutd-9aec1.firebaseapp.com",
  projectId: "hackutd-9aec1",
  storageBucket: "hackutd-9aec1.firebasestorage.app",
  messagingSenderId: "187748899748",
  appId: "1:187748899748:web:c0e217d49f1bee806e7f31"
};

// Initialize Firebase
const app = initializeApp(firebaseConfig);

export const db = getFirestore(app);
export const auth = getAuth(app);
export const googleProvider = new GoogleAuthProvider();
export default app;