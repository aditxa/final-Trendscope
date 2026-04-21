# Trendscope

Trendscope is a modern full-stack web application designed for trend aggregation, analysis, and discovery. It leverages an Express + Node.js backend acting as an API, serving a fast React frontend powered by Vite and Tailwind CSS. The app aggregates diverse data sources (such as Reddit, Google Trends, and GDELT) and utilizes AI classifications via the Gemini API to analyze trending topics.

## Tech Stack
- **Frontend**: React, Vite, Tailwind CSS, Radix UI (for accessible components), Recharts (for data visualization)
- **Backend**: Express (Node.js) acting as both the API server and serving the frontend bundle.
- **Database**: PostgreSQL with Drizzle ORM.
- **AI Integrations**: `@google/generative-ai` for intelligent classification.
- **Data Ingestion pipelines**: Scripts written in both TypeScript (`server/pipelines/`) and Python (`gdelt/`, `google_trends.py`, `classify_trends.py`) to scrape, parse, and analyze trends.

## Getting Started

### Prerequisites
- [Node.js](https://nodejs.org/) (v18.x or later recommended)
- `npm` (comes with Node.js)
- A PostgreSQL database (for storing the trends data)
- Required API keys (e.g., Gemini, etc. as defined in your `.env` file)

### Installation

1. Clone or navigate to the project directory:
   ```bash
   cd final-Trendscope
   ```

2. Install the necessary Node dependencies:
   ```bash
   npm install
   ```

3. Configure your environment:
   Make sure to create or update your `.env` file with your database connection URL (e.g., `DATABASE_URL`) and any required API Keys.

### Running the Application Locally

To start the application in development mode (which provides hot-reloading for both the frontend and backend), use:

```bash
npm run dev
```
Once you see `[express] serving on port 5001` (or whichever port is assigned), open your browser and navigate to:
**http://localhost:5001**

### Database Workflows

Whenever you make structural changes to your database schema using Drizzle, you can push them to your PostgreSQL database using:
```bash
npm run db:push
```

## Deployment

The application is configured to be deployed as a single unit where the backend Express server additionally serves the built frontend client.

1. **Build the project**:
   ```bash
   npm run build
   ```
   This will run `vite build` for the client and compile the TypeScript backend code.

2. **Start the production server**:
   ```bash
   npm start
   ```
   This command starts the Node.js production server. Ensure that in your production environment the `NODE_ENV` is set to `production`, `DATABASE_URL` is properly configured, and your application is exposed publicly through the required port.
