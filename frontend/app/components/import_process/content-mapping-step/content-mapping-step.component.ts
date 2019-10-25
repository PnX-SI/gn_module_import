import { Component, OnInit, OnChanges, Input } from '@angular/core';
import { FormControl, FormGroup, FormBuilder } from '@angular/forms';
import { StepsService } from '../steps.service';

@Component({
	selector: 'content-mapping-step',
	styleUrls: [ 'content-mapping-step.component.scss' ],
	templateUrl: 'content-mapping-step.component.html'
})
export class ContentMappingStepComponent implements OnInit, OnChanges {

	public isCollapsed = false;
	@Input() contentMappingInfo: any;
	contentForm: FormGroup;
	showForm: boolean = false;

	constructor(private stepService: StepsService, private _fb: FormBuilder) {}

	ngOnInit() {
		this.contentForm = this._fb.group({});
	}

	ngOnChanges() {
		if (this.contentMappingInfo) {
			this.contentMappingInfo.forEach((ele) => {
				ele['nomenc_values_def'].forEach((nomenc) => {
					this.contentForm.addControl(nomenc.name, new FormControl(''));
				});
			});
			this.showForm = true;
		}
	}

	onSelectChange(selectedVal, group) {
		this.contentMappingInfo.map((ele) => {
			if (ele.nomenc_abbr === group.nomenc_abbr)
			{
				ele.user_values.values = ele.user_values.values.filter(
					(value) => {
					return value.id !=selectedVal.id;
				});
			}
		})
	}

	onSelectDelete(deltetdVal, group) {
		this.contentMappingInfo.map((ele) => {
			if (ele.nomenc_abbr === group.nomenc_abbr)
			{
				let temp_array = ele.user_values.values;
				temp_array.push(deltetdVal);
				ele.user_values.values = temp_array;
			}
		})
	}

	onStepBack() {
		this.stepService.previousStep();
	}
}
