import { GoogleGenerativeAI } from "@google/generative-ai";

const apiKey = process.env.GEMINI_API_KEY;

if (!apiKey) {
  console.warn("GEMINI_API_KEY is not set. Recipe generation will fail.");
}

const genAI = new GoogleGenerativeAI(apiKey || "");
export const geminiModel = genAI.getGenerativeModel({
  model: "gemini-2.5-flash",
  generationConfig: {
    responseMimeType: "application/json",
  },
});

export async function generateRecipe(name: string, description: string, indianAlternativeText: string) {
  const prompt = `
    You are an expert culinary AI for a project called Food-Trend-Scout in India.
    Generate a precise, high-end recipe for the trending dish: "${name}".
    Description Context: ${description}
    Known Indian Alternative Idea: ${indianAlternativeText}
    
    CRITICAL: For each ingredient, determine if it is "hard to source" for an average home cook in India. If it is hard to source, set isHardToSource to true and provide a culturally-appropriate, accessible Indian alternative in 'indianAlternative'. If it is easily available, set isHardToSource to false and leave 'indianAlternative' empty.
    
    Return EXACTLY a JSON object matching this schema:
    {
      "ingredients": [
        { "name": "string", "amount": "string", "isHardToSource": boolean, "indianAlternative": "string" }
      ],
      "steps": ["string"]
    }
    Ensure the steps are detailed and written like a premium cookbook.
  `;

  try {
    const response = await geminiModel.generateContent(prompt);
    const jsonText = response.response.text();
    return JSON.parse(jsonText);
  } catch (error) {
    console.error("Gemini AI Recipe Generation Failed:", error);
    throw new Error("Failed to generate recipe using Gemini API.");
  }
}

export async function enrichTrend(phrase: string): Promise<{ description: string, originalDish: string, indianAlternative: string }> {
  if (!apiKey) {
    return {
      description: `A trending food item detected across global news: ${phrase}.`,
      originalDish: phrase,
      indianAlternative: `Localized version of ${phrase}`
    };
  }

  const prompt = `
    You are an expert culinary AI for Food-Trend-Scout.
    We detected a new viral food phrase from global news patterns: "${phrase}".
    
    Please provide:
    1. A short, punchy 1-2 sentence description explaining what this trend likely is and why it's viral.
    2. The 'originalDish' it is based on (e.g. if phrase is "Matcha Tiramisu", originalDish is "Tiramisu").
    3. An 'indianAlternative' idea that localizes this trend for Indian home cooks (e.g. Indian ingredients or formats).
    
    Return EXACTLY a JSON object matching this schema:
    {
      "description": "string",
      "originalDish": "string",
      "indianAlternative": "string"
    }
  `;

  try {
    const response = await geminiModel.generateContent(prompt);
    const jsonText = response.response.text();
    return JSON.parse(jsonText);
  } catch (error) {
    console.error("Gemini AI Trend Enrichment Failed:", error);
    return {
      description: `A trending food item detected across global news: ${phrase}.`,
      originalDish: phrase,
      indianAlternative: `Localized version of ${phrase}`
    };
  }
}
