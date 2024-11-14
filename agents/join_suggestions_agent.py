from typing import List, Dict
import pandas as pd
from sfn_blueprint import SFNAgent, Task, SFNOpenAIClient, SFNPromptManager
from config.model_config import MODEL_CONFIG
from validators.join_field_validator import JoinFieldValidator
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
        print(f"Metadata goin to agent>>: {metadata}")
        initial_suggestions = self._generate_initial_suggestions(metadata)
        
        suggestion_count = len(initial_suggestions)
        
        # Perform detailed validation checks
        validator = JoinFieldValidator(task.data['table1'], task.data['table2'])
        verification_results = self._verify_suggestions(
            initial_suggestions,
            validator
        )
        
        # Generate final recommendations
        final_recommendations = self._generate_final_recommendations(
            initial_suggestions,
            verification_results
        )
        
        return {
            'suggestion_count': suggestion_count,
            'initial_suggestions': initial_suggestions,
            'verification_results': verification_results,
            'final_recommendations': final_recommendations,
            'join_health_metrics': self._calculate_join_health(verification_results)
        }

    def _extract_metadata(self, df1: pd.DataFrame, df2: pd.DataFrame) -> Dict:
        """Extract metadata from both tables."""
        return {
            "table1_metadata": {
                "column_names": df1.columns.tolist(),
                "sample_data": df1.head().to_dict(),
                "data_types": df1.dtypes.to_dict(),
                "unique_counts": df1.nunique().to_dict()
            },
            "table2_metadata": {
                "column_names": df2.columns.tolist(),
                "sample_data": df2.head().to_dict(),
                "data_types": df2.dtypes.to_dict(),
                "unique_counts": df2.nunique().to_dict()
            }
        }

    def _verify_suggestions(self, suggestions: List[Dict], validator: JoinFieldValidator) -> Dict:
        """Verify suggested joins using the validator."""
        verification_results = {}
        
        for suggestion in suggestions:
            # Check date mapping
            if 'date_mapping' in suggestion:
                field1 = suggestion['date_mapping']['table1_field']
                field2 = suggestion['date_mapping']['table2_field']
                verification_results[f"{field1}_{field2}"] = validator.run_all_checks(
                    field1, field2, is_date=True
                )
            
            # Check customer mapping
            if 'customer_mapping' in suggestion:
                field1 = suggestion['customer_mapping']['table1_field']
                field2 = suggestion['customer_mapping']['table2_field']
                verification_results[f"{field1}_{field2}"] = validator.run_all_checks(
                    field1, field2, is_date=False
                )
            
            # Check product mapping if it exists
            if 'product_mapping' in suggestion:
                field1 = suggestion['product_mapping']['table1_field']
                field2 = suggestion['product_mapping']['table2_field']
                verification_results[f"{field1}_{field2}"] = validator.run_all_checks(
                    field1, field2, is_date=False
                )
        
        return verification_results

    def _calculate_join_health(self, verification_results: Dict) -> Dict:
        """Calculate overall join health metrics."""
        health_metrics = {}
        
        for join_pair, results in verification_results.items():
            health_metrics[join_pair] = {
                'uniqueness_score': self._calculate_uniqueness_score(results['uniqueness']),
                'overlap_score': results['value_overlap']['metrics']['overlap_percentage'],
                'null_impact': self._calculate_null_impact(results['null_analysis']),
                'overall_health': 0.0  # Will be calculated as weighted average
            }
            
            # Calculate overall health
            health_metrics[join_pair]['overall_health'] = (
                health_metrics[join_pair]['uniqueness_score'] * 0.4 +
                health_metrics[join_pair]['overlap_score'] * 0.4 +
                (100 - health_metrics[join_pair]['null_impact']) * 0.2
            )
        
        return health_metrics

    def _calculate_uniqueness_score(self, uniqueness_results: Dict) -> float:
        """Calculate uniqueness score from results."""
        table1_rate = uniqueness_results['table1_metrics']['metrics']['duplication_rate']
        table2_rate = uniqueness_results['table2_metrics']['metrics']['duplication_rate']
        return 100 - ((table1_rate + table2_rate) / 2)

    def _calculate_null_impact(self, null_results: Dict) -> float:
        """Calculate impact of null values."""
        table1_nulls = null_results['table1_nulls']['metrics']['null_percentage']
        table2_nulls = null_results['table2_nulls']['metrics']['null_percentage']
        return (table1_nulls + table2_nulls) / 2
    

    def _generate_initial_suggestions(self, metadata: Dict) -> List[Dict]:
        """Generate initial join suggestions using OpenAI."""
        # Get prompts using PromptManager
        system_prompt, user_prompt = self.prompt_manager.get_prompt(
            agent_type='initial_join_suggestions_generator',
            llm_provider='openai',
            **metadata
        )
        
        # Get suggestions from OpenAI
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
        response=response.choices[0].message.content
        print(f"Response initial initial suggestions>>: {response}")
        # Parse and structure the suggestions
        suggestions = self._parse_suggestions(response, metadata['table1_metadata']['column_names'], metadata['table2_metadata']['column_names'])
        return suggestions

    def _parse_suggestions(self, response: str, table1_columns: List[str], table2_columns: List[str]) -> List[Dict]:
        """
        Parse OpenAI response into structured suggestions.
        """
        try:
            # Clean the response to extract only the JSON content
            cleaned_response = response.strip()
            print(f"Cleaned response>>: {cleaned_response}")
            # Remove any markdown code block indicators
            cleaned_response = cleaned_response.replace('```json', '').replace('```', '')
            print(f"Cleaned response after removing markdown>>: {cleaned_response}")
            # Find the first and last curly braces
            start_idx = cleaned_response.find('{')
            end_idx = cleaned_response.rfind('}')
            if start_idx != -1 and end_idx != -1:
                cleaned_response = cleaned_response[start_idx:end_idx + 1]
            print(f"Cleaned response after finding curly braces>>: {cleaned_response}")
            suggestions_dict = json.loads(cleaned_response)
            
            # Rest of the existing parsing logic remains the same
            parsed_suggestions = []
            for suggestion_key, suggestion_data in suggestions_dict.items():
                # Skip if any required field is missing
                if not all(field in suggestion_data for field in ['DateField', 'CustIDField', 'ProdID']):
                    continue
                    
                # Skip if DateField or CustIDField don't have both table1 and table2
                if not all(key in suggestion_data['DateField'] for key in ['table1', 'table2']):
                    continue
                if not all(key in suggestion_data['CustIDField'] for key in ['table1', 'table2']):
                    continue
                
                # Get the suggested column names
                date_t1 = suggestion_data['DateField']['table1']
                date_t2 = suggestion_data['DateField']['table2']
                cust_t1 = suggestion_data['CustIDField']['table1']
                cust_t2 = suggestion_data['CustIDField']['table2']
                
                # Validate columns exist in respective tables
                if not (date_t1 in table1_columns and date_t2 in table2_columns):
                    continue
                if not (cust_t1 in table1_columns and cust_t2 in table2_columns):
                    continue
                
                # Skip if any required fields have null values
                if not date_t1 or not date_t2 or not cust_t1 or not cust_t2:
                    continue
                    
                suggestion = {
                    'date_mapping': {
                        'table1_field': date_t1,
                        'table2_field': date_t2
                    },
                    'customer_mapping': {
                        'table1_field': cust_t1,
                        'table2_field': cust_t2
                    }
                }
                
                # Add product mapping only if both fields exist, are not null, and exist in tables
                if ('ProdID' in suggestion_data and 
                    suggestion_data['ProdID'].get('table1') and 
                    suggestion_data['ProdID'].get('table2')):
                    prod_t1 = suggestion_data['ProdID']['table1']
                    prod_t2 = suggestion_data['ProdID']['table2']
                    
                    if prod_t1 in table1_columns and prod_t2 in table2_columns:
                        suggestion['product_mapping'] = {
                            'table1_field': prod_t1,
                            'table2_field': prod_t2
                        }
                    
                parsed_suggestions.append(suggestion)
            print(f"\n \n Parsed suggestions>>: {parsed_suggestions}")   
            return parsed_suggestions
            
        except Exception as e:
            print(f"Error parsing suggestions: {str(e)}")
            return []

    def _generate_final_recommendations(self, initial_suggestions: List[Dict], 
                                    verification_results: Dict) -> str:
        """Generate final recommendations using OpenAI."""
        system_prompt, user_prompt = self.prompt_manager.get_prompt(
            agent_type='final_join_suggestions_generator',
            llm_provider='openai',
            initial_suggestions=initial_suggestions,
            verification_results=verification_results
        )
        
        response = self.client.chat.completions.create(
            model=self.model_config["model"],
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt}
            ],
            temperature=0.1,  # Lower temperature for more consistent output
            max_tokens=self.model_config["max_tokens"],
            n=1,
            stop=self.model_config["stop"]
        )
        
        response = response.choices[0].message.content
        print(f"Response final recommendations>>: {response}")
        
        # Clean the response similar to initial suggestions
        cleaned_response = response.strip()
        # Remove markdown code block indicators
        cleaned_response = cleaned_response.replace('```json', '').replace('```', '')
        
        # Find the first and last curly braces
        start_idx = cleaned_response.find('{')
        end_idx = cleaned_response.rfind('}')
        if start_idx != -1 and end_idx != -1:
            cleaned_response = cleaned_response[start_idx:end_idx + 1]
        
        # Validate JSON format
        try:
            recommendations = json.loads(cleaned_response)
            return json.dumps(recommendations)  # Return properly formatted JSON string
        except json.JSONDecodeError:
            raise ValueError("Invalid JSON response from LLM")