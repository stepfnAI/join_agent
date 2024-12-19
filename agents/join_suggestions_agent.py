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

    def _normalize_date_column(self, df: pd.DataFrame, column_name: str) -> pd.Series:
        """
        Normalize date column to YYYY-MM format handling various input formats.
        Raises ValueError if the column doesn't appear to contain valid dates.
        """
        try:
            # Get sample of non-null values for analysis
            sample_values = df[column_name].dropna().head(5)
            print(f"Sample values for {column_name}:", sample_values.tolist())
            
            # Early validation - check if column appears to be non-date
            if df[column_name].dtype in ['int64', 'float64'] or column_name.lower().endswith(('_id', '_seats', '_qty', '_amount')):
                raise ValueError(f"Column '{column_name}' appears to be numeric or non-date. Please select a valid date column.")
            
            # Try to convert to datetime
            dates = pd.to_datetime(df[column_name], format='mixed', errors='coerce')
            
            # If more than 20% of non-null values failed to convert to dates, it's probably not a date column
            non_null_count = df[column_name].notna().sum()
            failed_conversion_rate = dates.isna().sum() / non_null_count if non_null_count > 0 else 1.0
            
            if failed_conversion_rate > 0.2:  # More than 20% conversion failure
                raise ValueError(
                    f"Column '{column_name}' does not appear to contain valid dates. "
                    f"Sample values: {sample_values.tolist()}. "
                    f"Please select a valid date column."
                )
            
            # Convert to YYYY-MM format
            return dates.dt.strftime('%Y-%m')
            
        except Exception as e:
            print(f"Error normalizing dates in {column_name}. Sample values: {sample_values.tolist()}")
            print(f"Error details: {str(e)}")
            raise ValueError(f"Failed to process column '{column_name}' as dates. {str(e)}")

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
            
            # Individual field overlap checks
            for prompt_key, verify_key in field_mappings.items():
                if prompt_key in suggestion and suggestion[prompt_key].get('table1') and suggestion[prompt_key].get('table2'):
                    field1 = suggestion[prompt_key]['table1']
                    field2 = suggestion[prompt_key]['table2']
                    
                    # Skip if fields don't exist in dataframes
                    if field1 not in df1.columns or field2 not in df2.columns:
                        continue
                    
                    # Special handling for date fields
                    if verify_key == 'date_mapping':
                        try:
                            # Convert to datetime and then to period for month-level comparison
                            dates1 = pd.to_datetime(df1[field1]).dt.to_period('M')
                            dates2 = pd.to_datetime(df2[field2]).dt.to_period('M')
                            values1 = set(dates1.dropna().unique())
                            values2 = set(dates2.dropna().unique())
                        except Exception as e:
                            print(f"Date conversion error: {str(e)}")
                            continue
                    else:
                        values1 = set(df1[field1].dropna().unique())
                        values2 = set(df2[field2].dropna().unique())
                    
                    overlap = values1.intersection(values2)
                    
                    verification_results[f"{field1}_{field2}"] = {
                        "overlap_percentage": len(overlap) / max(len(values1), len(values2)) * 100,
                        "total_values_table1": len(values1),
                        "total_values_table2": len(values2),
                        "overlapping_values": len(overlap)
                    }
            
            # Combined fields overlap check
            if suggestion:
                # Create temporary copies for date normalization
                df1_temp = df1.copy()
                df2_temp = df2.copy()
                
                # First, create normalized date columns
                if 'DateField' in suggestion and suggestion['DateField'].get('table1') and suggestion['DateField'].get('table2'):
                    date_field1 = suggestion['DateField']['table1']
                    date_field2 = suggestion['DateField']['table2']
                    try:
                        # Use the new normalization function
                        df1_temp[date_field1] = self._normalize_date_column(df1, date_field1)
                        df2_temp[date_field2] = self._normalize_date_column(df2, date_field2)
                    except Exception as e:
                        print(f"Date normalization error: {str(e)}")
                        verification_results[f"{date_field1}_{date_field2}"] = {
                            "error": f"Failed to normalize dates: {str(e)}"
                        }
                        continue
                
                # Prepare merge conditions
                merge_left_on = []
                merge_right_on = []
                
                # Add customer fields
                if 'CustIDField' in suggestion and suggestion['CustIDField'].get('table1') and suggestion['CustIDField'].get('table2'):
                    merge_left_on.append(suggestion['CustIDField']['table1'])
                    merge_right_on.append(suggestion['CustIDField']['table2'])
                
                # Add date fields (using original column names since we've normalized the data in place)
                if 'DateField' in suggestion and suggestion['DateField'].get('table1') and suggestion['DateField'].get('table2'):
                    merge_left_on.append(suggestion['DateField']['table1'])
                    merge_right_on.append(suggestion['DateField']['table2'])
                
                # Add product fields if they exist
                if 'ProdID' in suggestion and suggestion['ProdID'].get('table1') and suggestion['ProdID'].get('table2'):
                    merge_left_on.append(suggestion['ProdID']['table1'])
                    merge_right_on.append(suggestion['ProdID']['table2'])
                
                print("\nDebug - Merge Setup for suggestion:", suggestion_key)
                print("Merge left on:", merge_left_on)
                print("Merge right on:", merge_right_on)
                print("\nSample data before merge:")
                print("Left columns:", df1_temp[merge_left_on].head())
                print("Right columns:", df2_temp[merge_right_on].head())
                
                # Perform merge
                try:
                    merged_df = df1_temp.merge(
                        df2_temp,
                        left_on=merge_left_on,
                        right_on=merge_right_on,
                        how='inner'
                    )
                    
                    verification_results[f"combined_overlap_{suggestion_key}"] = {
                        "total_records_table1": len(df1),
                        "total_records_table2": len(df2),
                        "matching_records": len(merged_df),
                        "overlap_percentage": (len(merged_df) / min(len(df1), len(df2))) * 100
                    }
                except Exception as e:
                    print(f"Merge error for suggestion {suggestion_key}: {str(e)}")
                    verification_results[f"combined_overlap_{suggestion_key}"] = {
                        "error": str(e)
                    }
        
        print("\n\n>>>verification_results", verification_results)
        return verification_results

    def _generate_final_recommendations(self, suggestions: Dict, verification_results: Dict) -> str:
        """Generate final recommendations based on overlap verification"""
        # Check if any suggestions have matching records
            # Check if any suggestions have matching records
        has_valid_joins = False
        total_matching_records = 0
        
        # Check all combined overlap keys (combined_overlap_1, combined_overlap_2, etc.)
        for key in verification_results:
            if key.startswith('combined_overlap_') and verification_results[key]['matching_records'] > 0:
                total_matching_records += verification_results[key]['matching_records']
                has_valid_joins = True
        
        if not has_valid_joins or total_matching_records == 0:
            print("\n\n>>> No valid joins found - all combinations result in zero matching records")
            return {}  # Return empty JSON for zero overlap casehas_valid_joins = False
        
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
        
    def check_join_health(self, table1, table2, selected_join):
        """Check join health for manually selected columns"""
        verification_results = {}
        
        # Individual field overlap checks
        for mapping_type, mapping in selected_join.items():
            field1 = mapping['table1_field']
            field2 = mapping['table2_field']
            
            # Special handling for date fields
            if mapping_type == 'date_mapping':
                try:
                    # Use the same normalization method for consistency
                    dates1 = self._normalize_date_column(table1, field1)
                    dates2 = self._normalize_date_column(table2, field2)
                    
                    # Convert to datetime for range analysis
                    dates1_dt = pd.to_datetime(dates1)
                    dates2_dt = pd.to_datetime(dates2)
                    
                    # Calculate overlapping months
                    months1 = set(dates1_dt.dt.to_period('M'))
                    months2 = set(dates2_dt.dt.to_period('M'))
                    overlapping = months1.intersection(months2)
                    missing = months1.symmetric_difference(months2)
                    
                    verification_results[f"{field1}_{field2}"] = {
                        "mapping_type": "date_mapping",
                        "overlap_percentage": min(100, len(overlapping) / len(months1.union(months2)) * 100),
                        "date_range_table1": {
                            "start": dates1_dt.min().strftime('%Y-%m-%d'),
                            "end": dates1_dt.max().strftime('%Y-%m-%d')
                        },
                        "date_range_table2": {
                            "start": dates2_dt.min().strftime('%Y-%m-%d'),
                            "end": dates2_dt.max().strftime('%Y-%m-%d')
                        },
                        "overlapping_months": len(overlapping),
                        "missing_months": len(missing),
                        "missing_periods": [str(m) for m in sorted(missing)][:3]  # Sample of missing periods
                    }
                except Exception as e:
                    verification_results[f"{field1}_{field2}"] = {
                        "mapping_type": "date_mapping",
                        "error": str(e),
                        "overlap_percentage": 0,
                        "date_range_table1": {"start": "N/A", "end": "N/A"},
                        "date_range_table2": {"start": "N/A", "end": "N/A"},
                        "overlapping_months": 0,
                        "missing_months": 0,
                        "missing_periods": []
                    }
            else:
                # Original logic for non-date fields
                values1 = set(table1[field1].dropna().unique())
                values2 = set(table2[field2].dropna().unique())
                overlap = values1.intersection(values2)
                all_values = values1.union(values2)
                
                verification_results[f"{field1}_{field2}"] = {
                    "mapping_type": mapping_type,
                    "overlap_percentage": min(100, len(overlap) / len(all_values) * 100),
                    "unique_values_table1": len(values1),
                    "unique_values_table2": len(values2),
                    "overlapping_values": len(overlap),
                    "total_unique_values": len(all_values)
                }

        # Combined overlap metrics
        total_possible_combinations = min(
            len(table1),  # rows in table1
            len(table2)   # rows in table2
        )
        
        verification_results['combined_overlap'] = {
            "total_records_table1": len(table1),
            "total_records_table2": len(table2),
            "matching_records": 0,
            "overlap_percentage": 0,
            "multiplication_factor": 0  # New metric to show record multiplication
        }

        try:
            # Create temporary copies and normalize dates for join
            table1_copy = table1.copy()
            table2_copy = table2.copy()
            
            # Normalize dates
            table1_copy['normalized_date'] = self._normalize_date_column(table1, selected_join['date_mapping']['table1_field'])
            table2_copy['normalized_date'] = self._normalize_date_column(table2, selected_join['date_mapping']['table2_field'])
            
            # Prepare merge conditions
            merge_conditions = [
                (selected_join['customer_mapping']['table1_field'], selected_join['customer_mapping']['table2_field']),
                ('normalized_date', 'normalized_date')
            ]
            
            if 'product_mapping' in selected_join:
                merge_conditions.append(
                    (selected_join['product_mapping']['table1_field'], selected_join['product_mapping']['table2_field'])
                )
            
            # Perform merge
            merged_df = table1_copy.merge(
                table2_copy,
                left_on=[m[0] for m in merge_conditions],
                right_on=[m[1] for m in merge_conditions],
                how='inner'
            )
            
            # Update metrics
            matching_records = len(merged_df)
            max_expected = max(len(table1), len(table2))
            multiplication_factor = matching_records / max_expected
            
            verification_results['combined_overlap'].update({
                "matching_records": matching_records,
                "overlap_percentage": min(100, (matching_records / min(len(table1), len(table2))) * 100),
                "multiplication_factor": round(multiplication_factor, 2),
                "has_duplicates": multiplication_factor > 1,
                "duplicate_warning": (
                    f"Join produces {matching_records - max_expected:,} additional records due to "
                    f"{multiplication_factor:.1f}x multiplication factor. This suggests a one-to-many "
                    "or many-to-many relationship between the tables."
                ) if multiplication_factor > 1 else ""
            })
            
        except Exception as e:
            print(f"Error in join health check: {str(e)}")
        
        return verification_results