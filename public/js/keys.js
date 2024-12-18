const isLocalhost = window.location.hostname === "127.0.0.1";

async function fetchKeys() {
  try {
    const response = await fetch(
      "https://api-getapikeys-bhro7jtuda-ey.a.run.app"
    );
    const keys = await response.json();
    return keys;
  } catch (error) {
    console.error("Error fetching API keys:", error);
    throw new Error("Failed to load API keys: " + error.message);
  }
}

export const keyLoaded = fetchKeys().then((keys) => {
  return keys.MAPBOX_API_KEY;
});

export const firebaseKeyLoaded = fetchKeys().then((keys) => {
  return keys.FIREBASE_API_KEY;
});
