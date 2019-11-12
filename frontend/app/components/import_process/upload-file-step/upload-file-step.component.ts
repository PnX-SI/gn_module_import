import { Component, OnInit, Input } from '@angular/core';
import { ActivatedRoute } from '@angular/router';
import { DataService } from '../../../services/data.service';
import { ToastrService } from 'ngx-toastr';
import { ModuleConfig } from '../../../module.config';
import { FormGroup, FormBuilder, Validators } from '@angular/forms';
import { StepsService } from '../steps.service';

@Component({
	selector: 'upload-file-step',
	styleUrls: [ 'upload-file-step.component.scss' ],
	templateUrl: 'upload-file-step.component.html'
})
export class UploadFileStepComponent implements OnInit {
	public fileName: string;
	public spinner: boolean = false;
	private skip: boolean = false;
	public uploadForm: FormGroup;
	public uploadFileErrors: any;
	public importConfig = ModuleConfig;

	@Input() importId: number;

	constructor(
		private _activatedRoute: ActivatedRoute,
		private _ds: DataService,
		private toastr: ToastrService,
		private _fb: FormBuilder,
		private stepService: StepsService
	) {}

	ngOnInit() {
		this.uploadForm = this._fb.group({
			file: [ null, Validators.required ],
			encodage: [ null, Validators.required ],
			srid: [ null, Validators.required ],
			separator: [ null, Validators.required ]
		});
		this.formListener();
	}

	onFileSelected(event: any) {
		this.uploadForm.patchValue({
			file: <File>event.target.files[0]
		});
		this.fileName = this.uploadForm.get('file').value.name;
	}

	onUpload(formValues: any) {
		this.uploadFileErrors = null;
		this.spinner = true;
		if (!this.skip) {
			this._ds
				.postUserFile(formValues, this._activatedRoute.snapshot.queryParams['datasetId'], this.importId)
				.subscribe(
					(res) => {
						this.skip = true;
						this.importId = res.importId;
						res.srid = formValues.srid;
						this.stepService.nextStep(this.uploadForm, 'one', res);
						this.spinner = false;
					},
					(error) => {
						this.spinner = false;
						if (error.statusText === 'Unknown Error') {
							this.toastr.error('ERROR: IMPOSSIBLE TO CONNECT TO SERVER (check your connexion)');
						} else {
							if (error.status == 500) {
								this.toastr.error(error.error);
							}
							if (error.status == 400) {
								this.uploadFileErrors = error.error;
							}
							if (error.status == 403) {
								this.toastr.error(error.error);
							}
						}
					}
				);
		} else {
			this.spinner = false;
			this.stepService.nextStep(this.uploadForm, 'one');
		}
	}

	formListener() {
		this.uploadForm.valueChanges.subscribe(() => {
			if (this.uploadForm.valid) {
				this.stepService.resetCurrentStep('one');
				this.skip = false;
			}
		});
	}
}
