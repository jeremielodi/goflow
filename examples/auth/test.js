// test_refresh_correct.js
const axios = require('axios');

const BASE_URL = 'http://localhost:8080';

async function test() {
    console.log('1. Logging in...');
    const loginRes = await axios.post(`${BASE_URL}/auth/login`, {
        email: 'admin@goflow.com',
        password: 'admin123'
    });
    const accessToken = loginRes.data.access_token;
    const refreshToken = loginRes.data.refresh_token;
    
    console.log('✅ Login successful');
    console.log(`   Access Token (15 min): ${accessToken.substring(0, 50)}...`);
    console.log(`   Refresh Token (7 days): ${refreshToken.substring(0, 50)}...`);
    
    console.log('\n2. Using ACCESS token for API call...');
    try {
        const meRes = await axios.get(`${BASE_URL}/users/me`, {
            headers: { 'Authorization': `Bearer ${accessToken}` }
        });
        console.log('✅ API call successful with access token');
    } catch (error) {
        console.log('❌ API call failed:', error.response?.data?.message);
    }
    
    console.log('\n3. Refreshing tokens using REFRESH token...');
    try {
        const refreshRes = await axios.post(`${BASE_URL}/auth/refresh`, {
            refresh_token: refreshToken  // ← Using REFRESH token here
        });
        
        console.log('✅ Tokens refreshed successfully');
        console.log(`   New Access Token: ${refreshRes.data.access_token.substring(0, 50)}...`);
        console.log(`   New Refresh Token: ${refreshRes.data.refresh_token.substring(0, 50)}...`);
        
        console.log('\n4. Testing new access token...');
        const meRes2 = await axios.get(`${BASE_URL}/users/me`, {
            headers: { 'Authorization': `Bearer ${refreshRes.data.access_token}` }
        });
        console.log('✅ New access token works!');
        
    } catch (error) {
        console.log('❌ Refresh failed:', error.response?.data);
        if (error.response?.data?.message?.includes('expected refresh token')) {
            console.log('   ⚠️ Make sure you are sending the REFRESH token, not the access token!');
        }
    }
}

test();