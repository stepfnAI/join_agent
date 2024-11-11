from typing import List, Dict
import pandas as pd
from sfn_blueprint import SFNAgent, Task, SFNOpenAIClient, SFNPromptManager
from config.model_config import MODEL_CONFIG
from validators.join_field_validator import JoinFieldValidator
import os

class SFNJoinSuggestionsAgent(SFNAgent):
    def __init__(self):
        super().__init__(name="Join Suggestion Generator", role="Data Join Advisor")
        self.client = SFNOpenAIClient()
        self.model_config = MODEL_CONFIG["join_suggestions_generator"]
        parent_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '../'))
        prompt_config_path = os.path.join(parent_path, 'config', 'prompt_config.json')
        self.prompt_manager = SFNPromptManager(prompt_config_path)

    def execute_task(self, task: Task) -> Dict:
        """Execute the join suggestion generation task."""
        if not isinstance(task.data, dict) or 'table1' not in task.data or 'table2' not in task.data:
            raise ValueError("Task data must be a dictionary containing 'table1' and 'table2' DataFrames")

        # Extract metadata and perform initial analysis
        metadata = self._extract_metadata(task.data['table1'], task.data['table2'])
        initial_suggestions = self._generate_initial_suggestions(metadata)
        
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
            field1 = suggestion['table1_field']
            field2 = suggestion['table2_field']
            is_date = suggestion['field_type'] == 'date'
            
            verification_results[f"{field1}_{field2}"] = validator.run_all_checks(
                field1, field2, is_date
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