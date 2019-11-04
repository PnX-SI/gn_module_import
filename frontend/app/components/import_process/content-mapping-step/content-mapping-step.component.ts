import { Component, OnInit, OnChanges, Input } from '@angular/core';
import { FormControl, FormGroup, FormBuilder } from '@angular/forms';
import { StepsService } from '../steps.service';
import { DataService } from '../../../services/data.service';
import { ToastrService } from 'ngx-toastr';

@Component({
	selector: 'content-mapping-step',
	styleUrls: [ 'content-mapping-step.component.scss' ],
	templateUrl: 'content-mapping-step.component.html'
})
export class ContentMappingStepComponent implements OnInit, OnChanges {

	public isCollapsed = false;
    @Input() contentMappingInfo: any;
    @Input() selected_columns: any;
    @Input() table_name: any;
    @Input() importId: any;
	contentForm: FormGroup;
	showForm: boolean = false;
    contentMapRes: any;

	constructor(
        private stepService: StepsService, 
        private _fb: FormBuilder,
        private _ds: DataService,
        private toastr: ToastrService
        ) {}


	ngOnInit() {
		this.contentForm = this._fb.group({});
	}


	ngOnChanges() {
		if (this.contentMappingInfo) {
			this.contentMappingInfo.forEach((ele) => {
				ele['nomenc_values_def'].forEach((nomenc) => {
                    this.contentForm.addControl(nomenc.id, new FormControl(''));
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
    

    onContentMapping(value) {
        this._ds.postContentMap(value, this.table_name, this.selected_columns, this.importId).subscribe(
            (res) => {		
                this.contentMapRes = res;
                console.log(this.contentMapRes);
                this.stepService.nextStep(this.contentForm, 'three', res);
            },
            (error) => {
                if (error.statusText === 'Unknown Error') {
                    // show error message if no connexion
                    this.toastr.error('ERROR: IMPOSSIBLE TO CONNECT TO SERVER (check your connexion)');
                } else {
                    // show error message if other server error
                    console.log(error);
                    this.toastr.error(error.error.message);
                }
            }
        );
    }
    
}