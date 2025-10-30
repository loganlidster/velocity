# Vercel Deployment Setup

## Quick Setup

1. **Install Vercel CLI** (if not already installed):
   ```bash
   npm install -g vercel
   ```

2. **Login to Vercel:**
   ```bash
   vercel login
   ```

3. **Deploy from the velocity folder:**
   ```bash
   cd C:\Users\logan\OneDrive\Desktop\velocity
   vercel
   ```

4. **Follow prompts:**
   - Set up and deploy? **Y**
   - Which scope? (your account)
   - Link to existing project? **N**
   - Project name? **velocity** (or whatever you prefer)
   - Directory? **./public**
   - Override settings? **N**

5. **Add custom domain:**
   ```bash
   vercel domains add lidster.co
   ```

## Or Use Vercel Dashboard

1. Go to: https://vercel.com/new
2. Import from GitHub: `loganlidster/velocity`
3. Root Directory: `public`
4. Click **Deploy**
5. Add custom domain: `lidster.co` in project settings

## Auto-Deploy

Once connected, every push to GitHub will auto-deploy to Vercel!

**Your site will be live at:**
- Vercel URL: `velocity.vercel.app` (or similar)
- Custom domain: `lidster.co` (after DNS setup)