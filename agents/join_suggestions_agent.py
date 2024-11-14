from typing import List, Dict
import pandas as pd
from sfn_blueprint import SFNAgent, Task, SFNOpenAIClient, SFNPromptManager
from config.model_config import MODEL_CONFIG
import os
import json

class SFNJoinSuggestionsAgent(SFNAgent):
    def __init__(self):
        super().__init__(name="Join Suggestion Generator", role="Data Join Advisor")
        self.client = SFNOpenAIClient()
        self.model_config = MODEL_CONFIG["join_suggestions_generator"]
        parent_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '../'))
        prompt_config_path = os.path.join(parent_path, 'config', 'prompt_config.json')
        self.prompt_manager = SFNPromptManager(prompt_config_path)

    def execute_task(self, task: Task) -> Dict:
        if not isinstance(task.data, dict) or 'table1' not in task.data or 'table2' not in task.data:
            raise ValueError("Task data must be a dictionary containing 'table1' and 'table2' DataFrames")

        # Extract metadata and perform initial analysis
        metadata = self._extract_metadata(task.data['table1'], task.data['table2'])
        initial_suggestions = self._generate_initial_suggestions(metadata)
        
        
        # Only verify value overlap for suggested joins
        verification_results = self._verify_value_overlap(
            initial_suggestions,
            task.data['table1'],
            task.data['table2']
        )
        
        # Generate final recommendations
        final_recommendations = self._generate_final_recommendations(
            initial_suggestions,
            verification_results
        )
        
        return {
            'suggestion_count': len(initial_suggestions),
            'initial_suggestions': initial_suggestions,
            'verification_results': verification_results,
            'final_recommendations': final_recommendations
        }

    def _extract_metadata(self, df1: pd.DataFrame, df2: pd.DataFrame) -> Dict:
        """Extract basic metadata from both dataframes"""
        def get_table_metadata(df: pd.DataFrame) -> Dict:
            return {
                'columns': list(df.columns),
                'sample_values': {col: df[col].dropna().head(3).tolist() for col in df.columns},
                'dtypes': {col: str(df[col].dtype) for col in df.columns}
            }
        
        return {
            'table1_metadata': get_table_metadata(df1),
            'table2_metadata': get_table_metadata(df2)
        }

    def _generate_initial_suggestions(self, metadata: Dict) -> Dict:
        """Generate initial join suggestions based on metadata"""
        system_prompt, user_prompt = self.prompt_manager.get_prompt('initial_join_suggestions_generator',llm_provider='openai',**metadata)
        
        # Get suggestions from LLM
        response = self.client.chat.completions.create(
            model=self.model_config["model"],
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt}
            ],
            temperature=self.model_config["temperature"],
            max_tokens=self.model_config["max_tokens"],
            n=self.model_config["n"],
            stop=self.model_config["stop"]
        )

        # Clean the response content
        response_text = response.choices[0].message.content.strip()
        response_text = response_text[response_text.find('{'):response_text.rfind('}')+1]
        
        try:
            suggestions = json.loads(response_text)
            print("\n\n >>>initial suggestions", suggestions)
            return suggestions  # Return parsed JSON object instead of string
        except json.JSONDecodeError:
            # Try one more time after removing markdown
            response_text = response_text.replace('```json', '').replace('```', '')
            try:
                suggestions = json.loads(response_text)
                print("\n\n >>>initial suggestions", suggestions)
                return suggestions  # Return parsed JSON object
            except json.JSONDecodeError:
                print("Failed to parse JSON response")
                return {}  # Return empty dict instead of empty list

    def _verify_value_overlap(self, suggestions: Dict, df1: pd.DataFrame, df2: pd.DataFrame) -> Dict:
        """Verify value overlap between suggested join fields"""
        verification_results = {}
        
        # Iterate through each suggestion (suggestion1, suggestion2, etc.)
        for suggestion_key, suggestion in suggestions.items():
            # Map the prompt format keys to verification keys
            field_mappings = {
                'DateField': 'date_mapping',
                'CustIDField': 'customer_mapping', 
                'ProdID': 'product_mapping'
            }
            
            for prompt_key, verify_key in field_mappings.items():
                if prompt_key in suggestion and suggestion[prompt_key].get('table1') and suggestion[prompt_key].get('table2'):
                    field1 = suggestion[prompt_key]['table1']
                    field2 = suggestion[prompt_key]['table2']
                    
                    # Skip if fields don't exist in dataframes
                    if field1 not in df1.columns or field2 not in df2.columns:
                        continue
                    
                    values1 = set(df1[field1].dropna().unique())
                    values2 = set(df2[field2].dropna().unique())
                    overlap = values1.intersection(values2)
                    
                    verification_results[f"{field1}_{field2}"] = {
                        "overlap_percentage": len(overlap) / max(len(values1), len(values2)) * 100,
                        "total_values_table1": len(values1),
                        "total_values_table2": len(values2),
                        "overlapping_values": len(overlap)
                    }
        print("\n\n>>>verification results",verification_results)
        return verification_results

    def _generate_final_recommendations(self, suggestions: Dict, verification_results: Dict) -> str:
        """Generate final recommendations based on overlap verification"""
        context = {
            'initial_suggestions': suggestions,
            'verification_results': verification_results
        }
        system_prompt, user_prompt = self.prompt_manager.get_prompt('final_join_suggestions_generator',llm_provider='openai',**context)
        
        print("\n\n>>>final recommendations prompt",user_prompt,system_prompt)
        response = self.client.chat.completions.create(
            model=self.model_config["model"],
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt}
            ],
            temperature=self.model_config["temperature"],
            max_tokens=self.model_config["max_tokens"],
            n=self.model_config["n"],
            stop=self.model_config["stop"]
        )
        
        try:
            recommendations = response.choices[0].message.content.strip()
            recommendations = recommendations.replace('```json', '').replace('```', '')
            print("\n\n>>>final recommendations",recommendations)
            try:
                return json.loads(recommendations)  # Return parsed JSON
            except json.JSONDecodeError:
                return "{}"  # Return empty JSON object if parsing fails
        except Exception as e:
            print(f"Error processing recommendations: {str(e)}")
            return "{}"